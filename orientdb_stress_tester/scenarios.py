import logging
import os
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence, Type

from orientdb_stress_tester import timed
from orientdb_stress_tester.core import LOG_FORMAT
from orientdb_stress_tester.orientdb import Odb, OdbServer, OdbServerPool
from orientdb_stress_tester.process import OrientDBServerPoolManager
from orientdb_stress_tester.restarter import (
    AbstractServerRestarter,
    AlternatingStopStartServerRestarter,
    RandomServerKiller,
    RandomServerRestarter,
    SequentialServerRestarter,
)
from orientdb_stress_tester.scenario import (
    Scenario,
    ScenarioAwareDockerCompose,
    ScenarioManager,
)
from orientdb_stress_tester.schema import (
    OdbClassDef,
    OdbIndexDef,
    OdbPropertyDef,
    OdbSchemaInstaller,
)
from orientdb_stress_tester.workload import (
    RecordTestDataManager,
    RecordTestDataWorkloadManager,
)


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

    @property
    @classmethod
    @abstractmethod
    def SCENARIO_NAME(cls) -> str:
        pass

    @abstractmethod
    def run(self, config: Dict[str, Any]) -> None:
        pass


class ScenarioWorkload:

    SCHEMA = [
        OdbClassDef(
            "Record",
            [
                OdbPropertyDef("id", "INTEGER"),
                OdbPropertyDef("prop_uq", "INTEGER"),
                OdbPropertyDef("prop_nuq", "INTEGER"),
                OdbPropertyDef("prop_ftx", "STRING"),
            ],
            [
                OdbIndexDef("id", ["id"], "UNIQUE"),
                OdbIndexDef("prop_uq", ["id", "prop_uq"], "UNIQUE"),
                OdbIndexDef("prop_nuq", ["prop_nuq"], "NOTUNIQUE"),
                OdbIndexDef("prop_ftx", ["prop_ftx"], "FULLTEXT ENGINE LUCENE"),
            ],
        )
    ]

    @staticmethod
    def enlist(
        scenario: Scenario,
        orientdb_server_pool: OdbServerPool,
        workload_record_count: int = 100,
        workload_readonly: bool = False,
        workload_validation_readonly: bool = False,
        **kwargs: int,
    ) -> None:
        odb = Odb(orientdb_server_pool, "_scenario")
        schema_installer = OdbSchemaInstaller(odb, ScenarioWorkload.SCHEMA)
        test_data_mgr = RecordTestDataManager(odb, workload_record_count)
        scenario.enlist(schema_installer, test_data_mgr)

        workload_mgr = RecordTestDataWorkloadManager(scenario, test_data_mgr, workload_readonly=workload_readonly, **kwargs)
        scenario.enlist_action(workload_mgr)

        def validate_workload(_: float) -> Optional[bool]:
            if workload_mgr.is_workload_failed():
                # TODO: Could do this with FATAL level in error reporter?
                logging.warning("Background workloads reported failure.")
                return None
            logging.info("Validating availability for data query/update")
            return test_data_mgr.validate_workload(workload_validation_readonly)

        scenario.enlist_validation(validate_workload)


@dataclass(frozen=True)
class OrientDBScenarioConfig:
    base_name: str
    host: str
    base_port: int
    user: str
    password: str
    server_count: int


class AbstractDockerComposeScenario(AbstractScenario):
    def __init__(
        self,
        scenario_name: str,
        odb_scenario_config: OrientDBScenarioConfig,
        scenario_length: float = 60,
        enable_workload: bool = False,
        **kwargs: Any,
    ):
        AbstractScenario._init_logging()
        if enable_workload:
            scenario_name = f"{scenario_name}-under-load"

        self.scenario_length = scenario_length
        self.logger = logging.getLogger(type(self).__name__)
        self.sm = ScenarioManager(os.getcwd())
        self.scenario = self.sm.new_scenario(scenario_name)
        self.dc = ScenarioAwareDockerCompose()
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
        self.scenario.enlist_validation(self.validate_cluster)

        if enable_workload:
            ScenarioWorkload.enlist(self.scenario, self.orientdb_server_pool, **kwargs)

    def run(self, config: Dict[str, Any]) -> None:
        self.prepare()
        self.scenario.run_in_scenario(self.run_scenario_body, config)

    def validate_cluster(self, timeout: float) -> Optional[bool]:
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


class StartStopScenario(AbstractDockerComposeScenario):
    """Start cluster, wait for HA to stabilise, run workload for scenario length, shut down."""

    SCENARIO_NAME = "basic-startup"

    def __init__(self, config: OrientDBScenarioConfig, restart_interval: int = 10, **kwargs: Any) -> None:
        super().__init__(StartStopScenario.SCENARIO_NAME, config, **kwargs)
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


class AbstractRestartingScenario(AbstractDockerComposeScenario):
    def __init__(
        self,
        name: str,
        restarter_factory: Callable[[OrientDBServerPoolManager], AbstractServerRestarter],
        config: OrientDBScenarioConfig,
        restart_interval: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, config, **kwargs)
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

    def _do_restart(self, _: timed.Timer) -> None:
        self.restarter.restart_next()


class RollingRestartScenario(AbstractRestartingScenario):
    """Sequentially restarts server nodes at intervals, validating HA status after each set of restarts."""

    SCENARIO_NAME = "rolling-restart"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            RollingRestartScenario.SCENARIO_NAME,
            SequentialServerRestarter,
            **kwargs,
        )

    def _do_restart(self, scenario_timer: timed.Timer) -> None:
        for _ in range(self.orientdb_server_pool.size()):
            if not scenario_timer.is_active():
                return
            self.restarter.restart_next()
            scenario_timer.invoke_timed_if_active(time.sleep, self.restart_interval)


class RandomKillScenario(AbstractRestartingScenario):
    """Kills a random server node at intervals."""

    SCENARIO_NAME = "random-kill"

    def __init__(self, config: OrientDBScenarioConfig, **kwargs: Any) -> None:
        super().__init__(RandomKillScenario.SCENARIO_NAME, RandomServerKiller, config, **kwargs)


class RandomRestartScenario(AbstractRestartingScenario):
    """Restarts a random server node at intervals."""

    SCENARIO_NAME = "random-restart"

    def __init__(self, config: OrientDBScenarioConfig, **kwargs: Any) -> None:
        super().__init__(
            RandomRestartScenario.SCENARIO_NAME,
            RandomServerRestarter,
            config,
            **kwargs,
        )


class AlternatingStopStartScenario(AbstractRestartingScenario):
    """Stops and starts a random node, waiting for HA status to stabilise after each operation."""

    SCENARIO_NAME = "alternating-stop-start"

    def __init__(self, config: OrientDBScenarioConfig, **kwargs: Any) -> None:
        super().__init__(
            AlternatingStopStartScenario.SCENARIO_NAME,
            AlternatingStopStartServerRestarter,
            **kwargs,
        )


class AllScenarios(AbstractScenario):
    """Runs all scenarios in sequence"""

    SCENARIO_NAME = "all"

    def __init__(self, **kwargs: Any) -> None:
        pass

    def run(self, config: Any) -> None:
        all_other_scenarios = [scen for scen in Scenarios.ALL_SCENARIOS if scen != AllScenarios]

        for scenario_constructor in all_other_scenarios:
            executable_scenario = scenario_constructor(**config)
            executable_scenario.run(config)
            print()


class Scenarios:

    ALL_SCENARIOS: Sequence[Type[AbstractScenario]] = [
        StartStopScenario,
        RandomRestartScenario,
        AlternatingStopStartScenario,
        RollingRestartScenario,
        RandomKillScenario,
        AllScenarios,
    ]
