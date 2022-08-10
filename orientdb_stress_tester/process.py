import logging
import re
import shutil
import signal
import subprocess
from pathlib import Path
from typing import IO, Optional, Sequence, Text, TextIO, Tuple

from orientdb_stress_tester import timed
from orientdb_stress_tester.concurrent import FirmThread
from orientdb_stress_tester.core import OdbException
from orientdb_stress_tester.docker import DockerCompose
from orientdb_stress_tester.orientdb import (
    OdbOfflineException,
    OdbServer,
    OdbServerPool,
)
from orientdb_stress_tester.scenario import Scenario, ScenarioAware
from orientdb_stress_tester.workload import OrientDBErrorClassifier


class StreamMonitor(FirmThread[Tuple[int, str]]):
    def __init__(
        self, service: str, stream: IO[str], transcript_path: Path, error_reporter: Scenario.ErrorReporter, start_line_no: int = 0
    ) -> None:
        super().__init__(name=f"OrientDB-StreamMonitor-{service}")
        self.service = service
        self.stream = stream
        self.transcript_path = transcript_path
        self.transcript_file: Optional[TextIO] = None
        self.error_reporter = error_reporter
        self.line_no = start_line_no
        self._current_message: Optional[Tuple[int, str]] = None
        self.log_line_start = re.compile(f".+{self.service}.+\\| \\d{{4}}-\\d{{2}}-\\d{{2}} ")

    def _locked_prepare_work(self) -> Optional[Tuple[int, str]]:
        line = self.stream.readline()
        if line == "":
            logging.debug("EOF in stream for %s", self.service)
            return None
        self.line_no += 1
        return (self.line_no, line)

    def _do_work(self, work: Tuple[int, str]) -> None:
        line_info = work
        _, line = line_info
        if not self.transcript_file:
            self.transcript_file = open(self.transcript_path, "a", encoding="UTF-8")
        self.transcript_file.write(line)
        self.transcript_file.flush()

        self._collate_log_message(line_info)

    def current_line_no(self) -> int:
        with self.lock:
            return self.line_no

    def is_log_message_start(self, line: str) -> bool:
        is_log_line = self.log_line_start.search(line)
        return (is_log_line is not None) and (is_log_line.pos == 0)

    def _collate_log_message(self, line_info: Tuple[int, str]) -> None:
        _, new_line = line_info
        is_new_message = self.is_log_message_start(new_line)
        if is_new_message or (not self._current_message):
            self.check_for_errors()
            self._current_message = line_info
        else:
            line_no, current_lines = self._current_message
            self._current_message = (line_no, current_lines + new_line)

    def check_for_errors(self) -> None:
        if not self._current_message:
            return
        line_no, lines = self._current_message
        err_msg = lines.rstrip()
        self.error_reporter.report_error(line_no, err_msg)
        self._current_message = None

    def _locked_on_terminate(self) -> None:
        self.check_for_errors()

        if self.transcript_file:
            self.transcript_file.close()
            self.transcript_file = None


class OrientDBServerMonitor(FirmThread[subprocess.Popen[Text]]):
    def __init__(self, service: str, transcript_path: Path, error_reporter: Scenario.ErrorReporter) -> None:
        super().__init__(name=f"OrientDB-ServerMonitor-{service}")
        self.service = service
        self.transcript_path = transcript_path
        self.error_reporter = error_reporter
        self.stream_mon: Optional[StreamMonitor] = None
        self.process: Optional[subprocess.Popen[Text]] = None

    def _locked_prepare_work(self) -> subprocess.Popen[Text]:
        start_line_no = 0
        if self.stream_mon:
            # Wait for previous process output
            self.stream_mon.wait_for_exit(1)
            start_line_no = self.stream_mon.current_line_no()

        self.process = subprocess.Popen[Text](
            ["docker", "compose", "logs", self.service, "--follow", "--since", "0m"],
            text=True,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
        )
        logging.debug("Started docker compose logs process %d", self.process.pid)
        assert self.process.stdout is not None
        self.stream_mon = StreamMonitor(
            self.service,
            self.process.stdout,
            self.transcript_path,
            self.error_reporter,
            start_line_no=start_line_no,
        )
        self.stream_mon.start()
        return self.process

    def _do_work(self, work: subprocess.Popen[Text]) -> None:
        proc = work
        proc.wait()

    def _locked_signal_stop(self) -> None:
        if self.process:
            try:
                logging.debug(
                    "Killing server monitor process %s for %s",
                    self.process.pid,
                    self.service,
                )
                # docker-compose doesn't respond to SIGTERM
                self.process.send_signal(signal.SIGHUP)
                self.process.wait(2)
            except subprocess.TimeoutExpired:
                # This can leave zombies
                logging.debug(
                    "Server monitor process %s failed to exit cleanly, killing...",
                    self.process.pid,
                )
                self.process.kill()

    def _locked_on_terminate(self) -> None:
        # Wait for stream pump to complete (should EOF on stdout)
        if self.stream_mon is not None and not self.stream_mon.wait_for_exit(5):
            assert self.process is not None
            logging.warning(
                "Stream monitor for %s did not complete. Server process still running? %s",
                self.service,
                (self.process.poll() is None),
            )


class OrientDBServerProcessManager:
    def __init__(
        self, dc: DockerCompose, server: OdbServer, data_dir: Path, transcript_path: Path, error_reporter: Scenario.ErrorReporter
    ) -> None:
        self.dc = dc
        self.server = server
        self.service = server.name
        self.data_dir = data_dir.resolve()
        self.transcript_path = transcript_path
        self.error_reporter = error_reporter
        self.running = False
        self.mon: Optional[OrientDBServerMonitor] = None

    def is_running(self) -> bool:
        return self.running

    def _set_running(self, run: bool) -> None:
        self.running = run
        self.server.set_running(run)

    def destroy(self) -> None:
        self.stop()
        self.rm()

    def start(self) -> None:
        self.mon = OrientDBServerMonitor(self.service, self.transcript_path, self.error_reporter)
        self.dc.start(self.service)
        self._set_running(True)
        self.mon.start()  # docker compose logs won't detect logs unless this occurs after start in case of previous kill

    def stop(self) -> None:
        self.dc.stop(self.service)
        self._set_running(False)
        if self.mon:
            self.mon.stop()
            self.mon = None

    def rm(self) -> None:
        self.dc.rm(self.service)

    def restart(self) -> None:
        self.dc.restart(self.service)

    def kill(self, kill_signal: signal.Signals) -> None:
        self.dc.kill(self.service, kill_signal)
        self._set_running(False)
        if self.mon:
            self.mon.stop()
            self.mon = None

    def clean_data(self) -> None:
        logging.info("Cleaning data directory for [%s]", self.service)
        shutil.rmtree(self.data_dir, ignore_errors=True)


class OrientDBServerPoolManager(ScenarioAware):
    def __init__(self, server_pool: OdbServerPool, scenario: Scenario, dc: DockerCompose, data_dir: Path) -> None:
        self.LOG = logging.getLogger("OrientDBServerPoolManager")
        self.server_pool = server_pool
        self.data_dir = data_dir.resolve()
        self.mgrs = [
            OrientDBServerProcessManager(
                dc,
                srv,
                self.data_dir / "databases" / srv.name,
                scenario.allocate_file(f"docker-{srv.name}.log"),
                scenario.error_reporter(srv.name, OrientDBErrorClassifier()),
            )
            for srv in server_pool.servers
        ]

    def mgr_for(self, service: str) -> OrientDBServerProcessManager:
        return next(m for m in self.mgrs if m.service == service)

    def start_all(self) -> None:
        for mgr in self.mgrs:
            mgr.start()

    def wait_for_available(self, timeout: float) -> Optional[bool]:
        return self.server_pool.wait_for_available(timeout)

    def wait_for_distributed(self, timeout: float) -> Optional[Sequence[bool]]:
        return self.server_pool.wait_for_distributed(timeout)

    def stop_all(self) -> None:
        for mgr in self.mgrs:
            mgr.stop()

    def clean_data(self) -> None:
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def backup_data(self, backup_file_base_path: Path) -> None:
        parent_dir = self.data_dir.parent
        shutil.make_archive(str(backup_file_base_path), "zip", root_dir=parent_dir, base_dir=self.data_dir.name)

    def on_scenario_begin(self, scenario: Scenario) -> None:
        self.clean_data()
        self.start_all()
        # FIXME: source begin timeout from somewhere...
        if not self.wait_for_available(15):
            raise OdbException("OrientDB Server Pool did not start in expected time")

        # HACK: Wait for vanilla 3.2.x builds that NPE on create db
        # logging.warning("HACK: waiting for a while for 3.2 to start before creating DB")
        # time.sleep(2)
        def attempt_create_db() -> Optional[bool]:
            try:
                self.server_pool.choose_first_server().create_db_if_not_exist("_scenario")
                return True
            except OdbOfflineException as e:
                logging.debug(
                    "Creating DB for HA stability failed. Will retry. %s[%s]",
                    type(e).__name__,
                    e,
                )
                return None

        if not timed.try_until(attempt_create_db, 5):
            raise OdbException("OrientDB Server Pool could not create scenario test DB")

        if not self.wait_for_distributed(60):
            raise OdbException("OrientDB Server Pool did reach stable HA status in expected time")

        # Log version info after HA available (otherwise you can't access the DB)
        db_info = self.server_pool.choose_first_server().get_db("_scenario")
        version = db_info["server"]["version"]
        build = db_info["server"]["build"]
        logging.info("Server version is %s : %s", version, build)

    def on_scenario_end(self, scenario: Scenario) -> None:
        self.stop_all()
        self.backup_data(scenario.allocate_file("orientdb-backup"))
