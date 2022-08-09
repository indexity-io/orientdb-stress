import logging
import os
import random
import re
import sys
import threading
import traceback
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Pattern,
    Sequence,
    Tuple,
    TypeVar,
)

from orientdb_stress_tester.concurrent import FirmThread
from orientdb_stress_tester.core import LOG_FORMAT
from orientdb_stress_tester.docker import DockerCompose


class ScenarioAware(ABC):
    @abstractmethod
    def on_scenario_begin(self, scenario: "Scenario") -> None:
        pass

    @abstractmethod
    def on_scenario_end(self, scenario: "Scenario") -> None:
        pass


class ScenarioError:
    class ErrorClassification(Enum):
        UNKNOWN = 1
        KNOWN = 2
        SUPPRESSED = 3

    def __init__(
        self, err_class: "ScenarioError.ErrorClassification", err_type: str, err_source: str, log_line_no: int, err_msg: str
    ):
        self.err_class = err_class
        self.err_type = err_type
        self.err_source = err_source
        self.log_line_no = log_line_no
        self.err_msg = err_msg


class ScenarioErrorReporter(ABC):
    @abstractmethod
    def get_error_count(self, error_class: ScenarioError.ErrorClassification) -> int:
        pass

    @abstractmethod
    def add_error(self, err: ScenarioError) -> None:
        pass


class ScenarioErrorSet(ScenarioErrorReporter):
    def __init__(self, name: str):
        self.name = name
        self._unknown_errors: List[ScenarioError] = []
        self._suppressed_errors: List[ScenarioError] = []
        self._known_errors: List[ScenarioError] = []
        self.lock = threading.Lock()

    def reset(self) -> None:
        with self.lock:
            self._unknown_errors = []
            self._suppressed_errors = []
            self._known_errors = []

    def has_errors(self, err_class: Optional[ScenarioError.ErrorClassification] = None) -> bool:
        if err_class is None:
            with self.lock:
                return bool(self._unknown_errors) or bool(self._known_errors) or bool(self._suppressed_errors)

        if err_class == ScenarioError.ErrorClassification.KNOWN:
            return self.known_error_count() > 0
        if err_class == ScenarioError.ErrorClassification.SUPPRESSED:
            return self.suppressed_error_count() > 0
        return self.unknown_error_count() > 0

    def get_errors(self, err_class: ScenarioError.ErrorClassification) -> Sequence[ScenarioError]:
        if err_class == ScenarioError.ErrorClassification.KNOWN:
            return self.known_errors()
        if err_class == ScenarioError.ErrorClassification.SUPPRESSED:
            return self.suppressed_errors()
        return self.unknown_errors()

    def get_error_count(self, error_class: ScenarioError.ErrorClassification) -> int:
        if error_class == ScenarioError.ErrorClassification.KNOWN:
            return self.known_error_count()
        if error_class == ScenarioError.ErrorClassification.SUPPRESSED:
            return self.suppressed_error_count()
        return self.unknown_error_count()

    def unknown_error_count(self) -> int:
        with self.lock:
            return len(self._unknown_errors)

    def known_error_count(self) -> int:
        with self.lock:
            return len(self._known_errors)

    def suppressed_error_count(self) -> int:
        with self.lock:
            return len(self._suppressed_errors)

    def unknown_errors(self) -> Sequence[ScenarioError]:
        with self.lock:
            return self._unknown_errors.copy()

    def known_errors(self) -> Sequence[ScenarioError]:
        with self.lock:
            return self._known_errors.copy()

    def suppressed_errors(self) -> Sequence[ScenarioError]:
        with self.lock:
            return self._suppressed_errors.copy()

    def add_error(self, err: ScenarioError) -> None:
        with self.lock:
            if err.err_class == ScenarioError.ErrorClassification.KNOWN:
                self._known_errors.append(err)
            elif err.err_class == ScenarioError.ErrorClassification.SUPPRESSED:
                self._suppressed_errors.append(err)
            else:
                self._unknown_errors.append(err)


class AbstractErrorClassifier:
    @staticmethod
    def _exc_regex(pattern: str) -> re.Pattern[str]:
        return re.compile(pattern, re.MULTILINE | re.DOTALL)

    def __init__(
        self,
        classification_patterns: Sequence[Tuple[ScenarioError.ErrorClassification, str, Pattern[str]]],
        unknown_error_name_patterns: Sequence[Pattern[str]],
    ):
        self.classification_patterns = classification_patterns
        self.unknown_error_name_patterns = unknown_error_name_patterns

    def classify(self, message: str) -> Optional[Tuple[ScenarioError.ErrorClassification, str]]:
        unknown_type = None
        for utm in self.unknown_error_name_patterns:
            unknown_type_match = utm.search(message)
            if unknown_type_match:
                unknown_type = "_".join(unknown_type_match.groups())
                break

        if unknown_type is None:
            # Not something we recognise as an error pattern
            return None

        # Try for a more accurate classification
        for suppr_class, suppr_type, supr_matcher in self.classification_patterns:
            if supr_matcher.search(message):
                return (suppr_class, suppr_type)

        return (ScenarioError.ErrorClassification.UNKNOWN, unknown_type)


class Scenario:
    class Phase(Enum):
        PREPARE = 1
        EXECUTE = 2
        END = 3

    class ErrorReporter(ScenarioErrorReporter):
        def __init__(self, scenario: "Scenario", name: str, error_classifier: AbstractErrorClassifier) -> None:
            self.scenario = scenario
            self.name = name
            self.error_classifier = error_classifier

        def get_error_count(self, error_class: ScenarioError.ErrorClassification) -> int:
            es = self.scenario.error_set(self.name)
            return es.get_error_count(error_class)

        def add_error(self, err: ScenarioError) -> None:
            es = self.scenario.error_set(self.name)
            es.add_error(err)

        def classify_error(self, msg: str) -> Optional[Tuple[ScenarioError.ErrorClassification, str]]:
            return self.error_classifier.classify(msg)

        def report_error(self, line_no: int, err_msg: str) -> None:
            classification = self.classify_error(err_msg)
            if not classification:
                return
            err_class, err_type = classification
            if err_class == ScenarioError.ErrorClassification.KNOWN:
                known_error_count = self.get_error_count(ScenarioError.ErrorClassification.KNOWN)
                logging.warning(
                    "Known error (%d for this run) of type [%s] at [%s:%d]",
                    known_error_count + 1,
                    err_type,
                    self.name,
                    line_no,
                )
            elif err_class == ScenarioError.ErrorClassification.SUPPRESSED:
                logging.debug(
                    "Suppressed error of type [%s] at [%s:%d]",
                    err_type,
                    self.name,
                    line_no,
                )
            else:
                unknown_error_count = self.get_error_count(ScenarioError.ErrorClassification.UNKNOWN)
                logging.error(
                    "Unknown error (%d for this run) of type [%s] at [%s:%d] : %s",
                    unknown_error_count + 1,
                    err_type,
                    self.name,
                    line_no,
                    err_msg,
                )

            self.add_error(ScenarioError(err_class, err_type, self.name, line_no, err_msg))

    def __init__(self, index: int, name: str, path: Path) -> None:
        self.index = index
        self.name = name
        self.path = path
        self.scenario_log_handler: Optional[logging.Handler] = None
        self.scenario_debug_log_handler: Optional[logging.Handler] = None
        self.members: List[ScenarioAware] = []
        self.started_members: List[ScenarioAware] = []
        self.actions: List[ScenarioAware] = []
        self.started_actions: List[ScenarioAware] = []
        self.validations: List[Callable[[float], Optional[Any]]] = []
        self.random_seed = os.getenv("RAND_SEED", random.randrange(sys.maxsize))
        self.random = random.Random(self.random_seed)
        self.error_sets: Dict[Scenario.Phase, Dict[str, ScenarioErrorSet]] = {}
        self._reset_errors(Scenario.Phase.PREPARE)
        self.failed = False

    def enlist(self, *scenario_aware: ScenarioAware) -> None:
        for sa in scenario_aware:
            self.members.append(sa)

    def enlist_action(self, *scenario_aware: ScenarioAware) -> None:
        for sa in scenario_aware:
            self.actions.append(sa)

    def enlist_validation(self, *validation: Callable[[float], Optional[Any]]) -> None:
        for v in validation:
            self.validations.append(v)

    def unenlist(self, *scenario_aware: ScenarioAware) -> None:
        for sa in scenario_aware:
            if sa in self.actions:
                self.actions.remove(sa)
            if sa in self.members:
                self.members.remove(sa)

    def allocate_file(self, name: str) -> Path:
        return self.path / name

    def _touch(self, file: str) -> None:
        self.allocate_file(file).touch()

    def error_reporter(self, name: str, error_classifier: AbstractErrorClassifier) -> "Scenario.ErrorReporter":
        return Scenario.ErrorReporter(self, name, error_classifier)

    def error_set(self, name: str) -> ScenarioErrorSet:
        current_phase_errors = self.error_sets[self.current_phase]
        es = current_phase_errors.get(name)
        if not es:
            es = ScenarioErrorSet(name)
            current_phase_errors[name] = es
        return es

    def fail_scenario(self) -> None:
        logging.critical("Failing scenario.")
        self.failed = True

    def run_in_scenario(self, scenario_body: Callable[[], None], config: Dict[str, Any]) -> None:
        try:
            self._begin(config)
        except Exception as e:  # pylint: disable=broad-except
            logging.error("Scenario begin failed with error %s[%s]", type(e).__name__, e)
            logging.debug(traceback.format_exc())
            self.fail_scenario()
            self._end()
            return

        try:
            scenario_body()
        except Exception as e:  # pylint: disable=broad-except
            logging.error("Scenario body failed with error [%s]", e)
            logging.debug(traceback.format_exc())
            self.fail_scenario()

            # TODO: Post scenario validation phase (e.g. test if server can respond to queries after scenario fails)?
            # TODO: Mark scenario failure (either fatal errors or overall state)
        self._end()

    def _begin(self, config: Dict[str, Any]) -> None:
        self.scenario_log_handler = logging.FileHandler(self.allocate_file("log.txt"))
        self.scenario_log_handler.setFormatter(LOG_FORMAT)
        self.scenario_log_handler.setLevel(logging.INFO)
        self.scenario_debug_log_handler = logging.FileHandler(self.allocate_file("log-debug.txt"))
        self.scenario_debug_log_handler.setFormatter(LOG_FORMAT)
        self.scenario_debug_log_handler.setLevel(logging.DEBUG)

        logging.getLogger().addHandler(self.scenario_log_handler)
        logging.getLogger().addHandler(self.scenario_debug_log_handler)
        logging.info("Running scenario [%d-%s]", self.index, self.name)
        logging.info("  Transcript in %s", self.path)
        logging.info("  RANDOM SEED: %s", self.random_seed)
        if config:
            logging.info("  Config options:")
            for name, value in sorted(config.items()):
                logging.info("    %-30s = %s", name, value)

        logging.info("Scenario preparation beginning...")
        for sa in self.members:
            self.started_members.append(sa)
            sa.on_scenario_begin(self)

        self._report_current_phase_errors()
        self._reset_errors(Scenario.Phase.EXECUTE)
        logging.info("Scenario execution beginning...")
        for sa in self.actions:
            self.started_actions.append(sa)
            sa.on_scenario_begin(self)

    def _end(self) -> None:
        logging.info("Scenario execution complete")
        self._report_current_phase_errors()
        self._reset_errors(Scenario.Phase.END)
        logging.info("Ending scenario [%d-%s] ...", self.index, self.name)
        for sa in reversed(self.started_actions):
            logging.debug("Ending sa %s", sa)
            sa.on_scenario_end(self)
        for sa in reversed(self.started_members):
            logging.debug("Ending sa %s", sa)
            sa.on_scenario_end(self)
        self._report_current_phase_errors()
        logging.info(
            "Completed scenario [%d-%s] with result: %s",
            self.index,
            self.name,
            "FAILED" if self.failed else "SUCCEEDED",
        )
        if self.failed:
            self._touch("failed")
        self._touch("completed")

        self._report_scenario_errors()
        if self.scenario_log_handler:
            logging.getLogger().removeHandler(self.scenario_log_handler)
            assert self.scenario_debug_log_handler is not None
            logging.getLogger().removeHandler(self.scenario_debug_log_handler)

    def validate(self, timeout: float) -> Optional[bool]:
        for v in self.validations:
            valid = v(timeout)
            if valid is None:
                return None
        return True

    def _has_errors(self) -> bool:
        current_phase_errors = self.error_sets[self.current_phase]
        return next((es for es in current_phase_errors.values() if es.has_errors()), None) is not None

    def _reset_errors(self, new_phase: "Scenario.Phase") -> None:
        self.current_phase = new_phase
        self.error_sets[self.current_phase] = {}

    def _report_current_phase_errors(self) -> None:
        if self._has_errors():
            logging.info("Errors reported during scenario %s:", self.current_phase.name)
            current_phase_errors = self.error_sets[self.current_phase]
            for es in sorted(current_phase_errors.values(), key=lambda es: es.name):
                if es.has_errors():
                    logging.info(
                        "%20s : [U:%3d, K:%3d, S:%3d]",
                        es.name,
                        es.unknown_error_count(),
                        es.known_error_count(),
                        es.suppressed_error_count(),
                    )
                else:
                    logging.debug(
                        "%20s : [U:%3d, K:%3d, S:%3d]",
                        es.name,
                        es.unknown_error_count(),
                        es.known_error_count(),
                        es.suppressed_error_count(),
                    )
                if es.unknown_error_count():
                    logging.debug("Unknown errors:")
                    for err in es.unknown_errors():
                        logging.debug(
                            "   [%s:%d] : %s",
                            err.err_source,
                            err.log_line_no,
                            err.err_msg,
                        )
                if es.known_error_count():
                    logging.debug("Known errors:")
                    for err in es.known_errors():
                        logging.debug(
                            "   %s [%s:%d] : %s",
                            err.err_type,
                            err.err_source,
                            err.log_line_no,
                            err.err_msg,
                        )
                if es.suppressed_error_count():
                    logging.debug("Suppressed errors:")
                    for err in es.suppressed_errors():
                        logging.debug(
                            "   %s [%s:%d] : %s",
                            err.err_type,
                            err.err_source,
                            err.log_line_no,
                            err.err_msg,
                        )

    def _report_scenario_errors(self) -> None:
        reportable_error_classes = [
            ScenarioError.ErrorClassification.UNKNOWN,
            ScenarioError.ErrorClassification.KNOWN,
        ]

        class_errors = {
            err_class: {phase: list[ScenarioErrorSet]() for phase in Scenario.Phase}
            for err_class in ScenarioError.ErrorClassification
        }
        class_phase_totals = {
            err_class: {phase: 0 for phase in Scenario.Phase} for err_class in ScenarioError.ErrorClassification
        }
        class_totals = {err_class: 0 for err_class in ScenarioError.ErrorClassification}
        err_total = 0

        for phase, phase_error_sets in self.error_sets.items():
            for err_class in reportable_error_classes:
                for _, es in phase_error_sets.items():
                    errs = es.get_errors(err_class)
                    if errs:
                        class_errors[err_class][phase].append(es)
                        class_phase_totals[err_class][phase] = class_phase_totals[err_class][phase] + len(errs)
                        class_totals[err_class] = class_totals[err_class] + len(errs)
                        err_total += 1

        if err_total:
            with open(self.allocate_file("errors"), "w", encoding="UTF-8") as error_file:
                error_file.write(f"Total errors: {err_total}\n")
                for err_class in reportable_error_classes:
                    error_file.write(f"{err_class.name}\n")
                    for phase in Scenario.Phase:
                        error_file.write(f"  {phase.name:7} : {class_phase_totals[err_class][phase]}\n")

            for err_class in reportable_error_classes:
                if class_totals[err_class]:
                    with open(self.allocate_file(f"errors_{err_class.name}"), "w", encoding="UTF-8") as error_file:
                        error_file.write(f"Total errors: {class_totals[err_class]}\n\n")
                        for phase in Scenario.Phase:
                            if class_phase_totals[err_class][phase]:
                                error_file.write(f"  {phase.name} : {class_phase_totals[err_class][phase]}\n\n")
                                for err_set in class_errors[err_class][phase]:
                                    error_file.write(f"  {err_set.name}:\n")
                                    for err in err_set.get_errors(err_class):
                                        error_file.write(f"    [{err.log_line_no}|{err.err_type}] : {err.err_msg}\n")
                                    error_file.write("\n")


class ScenarioManager:
    @staticmethod
    def _is_scenario_dir(path: Path) -> bool:
        return path.is_dir() and re.match("\\d+-.+", path.name) is not None

    def __init__(self, root_dir: str) -> None:
        self.root_dir = Path(root_dir).resolve()
        self.scenario_dir = self.root_dir / "scenarios"
        self.data_dir = self.root_dir / "data"
        self._init_scenario_store()

    def _init_scenario_store(self) -> None:
        self.scenario_dir.mkdir(exist_ok=True)

    def _next_scenario_index(self) -> int:
        scens = sorted(list(d.name for d in self.scenario_dir.iterdir() if ScenarioManager._is_scenario_dir(d)))
        if not scens:
            return 1
        scen = scens[-1]
        return int(scen[0 : scen.index("-")]) + 1

    def new_scenario(self, name: str) -> Scenario:
        next_idx = self._next_scenario_index()
        next_path = self.scenario_dir / f"{next_idx:03}-{name}"
        next_path.mkdir()
        return Scenario(next_idx, name, next_path)


W = TypeVar("W")


class ScenarioAwareFirmThread(FirmThread[W], ScenarioAware):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.scenario: Optional[Scenario] = None

    def on_scenario_begin(self, scenario: Scenario) -> None:
        self.scenario = scenario
        self.start()

    def on_scenario_end(self, scenario: Scenario) -> None:
        self.stop()

    def _on_terminate(self) -> None:
        super()._on_terminate()
        if self.scenario:
            logging.debug("Unenlisting %s", self)
            self.scenario.unenlist(self)
        logging.debug("Thread %s terminated", self.name)


class ScenarioAwareDockerCompose(DockerCompose, ScenarioAware):
    def on_scenario_begin(self, scenario: Scenario) -> None:
        self.down_all()

    def on_scenario_end(self, scenario: Scenario) -> None:
        pass
