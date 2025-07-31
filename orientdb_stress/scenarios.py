import logging
import os
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Type

from orientdb_stress import timed
from orientdb_stress.core import LOG_FORMAT
from orientdb_stress.docker import DockerComposeTemplates
from orientdb_stress.orientdb import Odb, OdbServer, OdbServerPool
from orientdb_stress.process import OrientDBServerPoolManager
from orientdb_stress.record import Record
from orientdb_stress.restarter import (
    AbstractServerRestarter,
    AlternatingStopStartServerRestarter,
    AbstractServerSelector,
    RandomServerSelector,
    StopStartServerRestarter,
    SequentialServerSelector,
)
from orientdb_stress.scenario import (
    Scenario,
    ScenarioAwareDockerCompose,
    ScenarioManager,
    ScenarioValidator,
)
from orientdb_stress.schema import OdbSchemaInstaller
from orientdb_stress.templates import Templates
from orientdb_stress.workload import (
    RecordTestDataManager,
    RecordTestDataWorkloadManager,
)


@dataclass(frozen=True)
class OrientDBScenarioConfig:
    base_name: str
    host: str
    base_port: int
    user: str
    password: str
    server_count: int


class AbstractScenario(ABC):
    _logging_initialised = False

    @classmethod
    def _init_logging(cls) -> None:
        if cls._logging_initialised:
            return
        cls._logging_initialised = True
        log_stdout = logging.StreamHandler(sys.stdout)
        log_stdout.setLevel(logging.INFO)
        log_stdout.setFormatter(LOG_FORMAT)
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(log_stdout)

    @staticmethod
    @abstractmethod
    def scenario_name() -> str:
        pass

    @abstractmethod
    def run(self, config: Dict[str, Any]) -> None:
        pass


class ScenarioWorkload:
    @staticmethod
    def enlist(
        scenario: Scenario,
        orientdb_server_pool: OdbServerPool,
        workload_record_count: int = 100,
        **kwargs: Any,
    ) -> None:
        odb = Odb(orientdb_server_pool, "_scenario")
        schema_installer = OdbSchemaInstaller(odb, Record.SCHEMA)
        test_data_mgr = RecordTestDataManager(odb, workload_record_count)
        scenario.enlist(schema_installer, test_data_mgr)

        workload_mgr = RecordTestDataWorkloadManager(
            scenario,
            test_data_mgr,
            **kwargs,
        )
        scenario.enlist_action(workload_mgr)
        scenario.enlist_validation(workload_mgr)


class OdbConfigTemplates:
    @staticmethod
    def generate(config_dir: Path, config: OrientDBScenarioConfig):
        # Load distributed config template
        distributed_template_name = "config/distributed-db-config.json.j2"
        distributed_template = Templates.load_template(distributed_template_name)
        distributed_config_file = config_dir / "distributed-db-config.json"

        context = vars(config)

        # Generate distributed config file
        distributed_template.generate(distributed_config_file, context)

        # Load and generate hazelcast config for each server
        hazelcast_template_name = "config/hazelcast.xml.j2"
        hazelcast_template = Templates.load_template(hazelcast_template_name)

        for server_index in range(1, config.server_count + 1):
            server_context = context.copy()
            server_context["server_index"] = server_index
            server_config_file = config_dir / f"hazelcast-{server_index}.xml"
            hazelcast_template.generate(server_config_file, server_context)


class AbstractDockerComposeScenario(AbstractScenario, ScenarioValidator, ABC):
    def __init__(
        self,
        scenario_name: str,
        odb_scenario_config: OrientDBScenarioConfig,
        scenario_length: float = 60,
        workload_enabled: bool = False,
        **kwargs: Any,
    ):
        AbstractScenario._init_logging()
        if workload_enabled:
            scenario_name = f"{scenario_name}-under-load"

        self.scenario_length = scenario_length
        self.logger = logging.getLogger(type(self).__name__)
        self.sm = ScenarioManager(os.getcwd())
        self.scenario = self.sm.new_scenario(scenario_name)
        dc_template_name = "docker-compose-orientdb"
        dc_template = DockerComposeTemplates.load_template(dc_template_name)
        dc_file = self.scenario.path / "docker-compose.yml"
        dc_template.generate(dc_file, vars(odb_scenario_config))
        self.dc = ScenarioAwareDockerCompose(dc_file)

        odb_scenario_config_dir = self.scenario.path / "config"
        odb_scenario_config_dir.mkdir(parents=True, exist_ok=True)
        OdbConfigTemplates.generate(odb_scenario_config_dir, odb_scenario_config)

        # noinspection HttpUrlsUsage
        self.orientdb_server_pool = OdbServerPool(
            [
                OdbServer(
                    f"{odb_scenario_config.base_name}{index}",
                    f"http://{odb_scenario_config.host}:{odb_scenario_config.base_port + index}",
                    odb_scenario_config.user,
                    odb_scenario_config.password,
                )
                for index in range(1, odb_scenario_config.server_count + 1)
            ],
            self.scenario.random,
        )
        self.server_pool_manager = OrientDBServerPoolManager(self.orientdb_server_pool, self.scenario, self.dc, self.sm.data_dir)
        self.scenario.enlist(self.dc, self.server_pool_manager)
        self.scenario.enlist_validation(self)

        if workload_enabled:
            ScenarioWorkload.enlist(self.scenario, self.orientdb_server_pool, **kwargs)

    def run(self, config: Dict[str, Any]) -> None:
        self.prepare()
        self.scenario.run_in_scenario(self.run_scenario_body, config)

    def validate(self, timeout: float) -> Optional[bool]:
        logging.info("Validating cluster state")
        results = timed.try_all_timed_until(
            [
                self.server_pool_manager.wait_for_available,
                self.server_pool_manager.wait_for_distributed,
            ],
            timeout,
        )
        if results is None:
            return None
        return True

    def prepare(self) -> None:
        pass

    @abstractmethod
    def run_scenario_body(self) -> None:
        pass


class BasicStartupScenario(AbstractDockerComposeScenario):
    """Start cluster, wait for HA to stabilise, run workload for scenario length, shut down."""

    @staticmethod
    def scenario_name() -> str:
        return "basic-startup"

    def __init__(self, config: OrientDBScenarioConfig, restart_interval: int = 10, **kwargs: Any) -> None:
        super().__init__(BasicStartupScenario.scenario_name(), config, **kwargs)
        self.restart_interval = restart_interval

    def run_scenario_body(self) -> None:
        def wait_and_validate(time_remaining: float) -> Optional[bool]:
            scenario_timer = timed.Timer(time_remaining)
            valid = self.scenario.validate(60)
            if valid is None:
                self.logger.critical("Scenario validation failed.")
                self.scenario.fail_scenario()
                return None
            scenario_timer.invoke_timed_if_active(time.sleep, self.restart_interval)
            return True

        timed.repeat_timed_until_failure(wait_and_validate, self.scenario_length)


class AbstractRestartingScenario(AbstractDockerComposeScenario, ABC):
    def __init__(
        self,
        name: str,
        selector_factory: Callable[[OrientDBServerPoolManager], AbstractServerSelector],
        restarter_factory: Callable[[OrientDBServerPoolManager], AbstractServerRestarter],
        config: OrientDBScenarioConfig,
        restart_interval: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, config, **kwargs)
        # noinspection PyArgumentList
        self.selector = selector_factory(self.server_pool_manager, **kwargs)
        # noinspection PyArgumentList
        self.restarter = restarter_factory(self.server_pool_manager, **kwargs)
        self.restart_interval = restart_interval

    def run_scenario_body(self) -> None:
        self.logger.debug(
            "Starting %s scenario, restart_interval=%d, scenario_length=%d",
            self.scenario.name,
            self.restart_interval,
            self.scenario_length,
        )

        def restart_and_validate(time_remaining: float) -> Optional[bool]:
            scenario_timer = timed.Timer(time_remaining)
            self._do_restart(scenario_timer)
            valid = self.scenario.validate(60)
            if valid is None:
                self.logger.critical("Scenario validation failed.")
                self.scenario.fail_scenario()
                return None
            scenario_timer.invoke_timed_if_active(time.sleep, self.restart_interval)
            return True

        timed.repeat_timed_until_failure(restart_and_validate, self.scenario_length)

    @abstractmethod
    def _do_restart(self, _: timed.Timer) -> None:
        pass


class AbstractServerRestartingScenario(AbstractRestartingScenario, ABC):
    def __init__(
        self,
        name: str,
        selector_factory: Callable[[OrientDBServerPoolManager], AbstractServerSelector],
        restarter_factory: Callable[[OrientDBServerPoolManager], AbstractServerRestarter],
        config: OrientDBScenarioConfig,
        restart_interval: int = 10,
        **kwargs: Any,
    ):
        super().__init__(name, selector_factory, restarter_factory, config, restart_interval, **kwargs)
        self._current_server: Optional[OdbServer] = None
        self._restart_complete = True

    def _do_restart(self, _: timed.Timer) -> None:
        if self._restart_complete:
            self._current_server = self.selector.choose_next_server()
        self._restart_complete = self.restarter.restart_server(self._current_server)


class RollingRestartScenario(AbstractRestartingScenario):
    """Sequentially restarts all server nodes at intervals, validating HA status after each set of restarts."""

    @staticmethod
    def scenario_name() -> str:
        return "rolling-restart"

    def __init__(self, config: OrientDBScenarioConfig, **kwargs: Any) -> None:
        super().__init__(
            RollingRestartScenario.scenario_name(),
            SequentialServerSelector,
            StopStartServerRestarter,
            config,
            **kwargs,
        )

    def _do_restart(self, scenario_timer: timed.Timer) -> None:
        for _ in range(self.orientdb_server_pool.size()):
            if not scenario_timer.is_active():
                return
            server = self.selector.choose_next_server()
            self.restarter.restart_server(server)
            scenario_timer.invoke_timed_if_active(time.sleep, self.restart_interval)


class RandomRestartScenario(AbstractServerRestartingScenario):
    """Restarts (stops and then starts) a server node at intervals."""

    @staticmethod
    def scenario_name() -> str:
        return "random-restart"

    def __init__(self, config: OrientDBScenarioConfig, **kwargs: Any) -> None:
        super().__init__(
            RandomRestartScenario.scenario_name(),
            RandomServerSelector,
            StopStartServerRestarter,
            config,
            **kwargs,
        )


class AlternatingStopStartScenario(AbstractServerRestartingScenario):
    """Stops and starts a random node, waiting for HA status to stabilise after each operation."""

    @staticmethod
    def scenario_name() -> str:
        return "alternate-stop-start"

    def __init__(self, config: OrientDBScenarioConfig, **kwargs: Any) -> None:
        super().__init__(
            AlternatingStopStartScenario.scenario_name(),
            RandomServerSelector,
            AlternatingStopStartServerRestarter,
            config,
            **kwargs,
        )


class AllScenarios(AbstractScenario):
    """Runs all scenarios in sequence"""

    @staticmethod
    def scenario_name() -> str:
        return "all"

    def __init__(self, odb_scenario_config: OrientDBScenarioConfig, **kwargs: Any) -> None:
        # noinspection PyArgumentList
        self.scenarios = [sc(odb_scenario_config, **kwargs) for sc in Scenarios.ALL_SCENARIOS if sc != AllScenarios]

    def run(self, config: Dict[str, Any]) -> None:
        for executable_scenario in self.scenarios:
            # print(f"{executable_scenario.SCENARIO_NAME()}")
            executable_scenario.run(config)
            print()


class Scenarios:

    ALL_SCENARIOS: Sequence[Type[AbstractScenario]] = [
        BasicStartupScenario,
        RandomRestartScenario,
        AlternatingStopStartScenario,
        RollingRestartScenario,
        AllScenarios,
    ]
