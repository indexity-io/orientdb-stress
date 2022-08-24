import logging
import random
import re
import traceback
from typing import Any, Optional, Sequence, Union

from orientdb_stress import timed
from orientdb_stress.concurrent import FirmThread
from orientdb_stress.core import OdbException
from orientdb_stress.orientdb import JsonObject, Odb, OdbRestException
from orientdb_stress.record import Record
from orientdb_stress.scenario import (
    AbstractErrorClassifier,
    Scenario,
    ScenarioAware,
    ScenarioError,
    ScenarioValidator,
)


class RecordTestDataManager(ScenarioAware):
    def __init__(self, odb: Odb, scale: int) -> None:
        self.odb = odb
        self.scale = scale
        self.validation_record_id = scale + 10000
        self.random: Optional[random.Random] = None

    def on_scenario_begin(self, scenario: Scenario) -> None:
        self.random = scenario.random
        # FIXME: propagage timeout from somewhere
        res = timed.try_all_until([self._try_clear_records, self._try_create_records], 60)
        if not res:
            raise OdbException("Record test data setup failed")

    def on_scenario_end(self, scenario: Scenario) -> None:
        pass

    def _try_clear_records(self) -> Optional[bool]:
        try:
            self.odb.sql("DELETE FROM Record")
            return True
        except OdbException as e:
            logging.warning("Resetting test records failed with error [%s]. Will retry.", e)
            logging.debug(traceback.format_exc())
            return None

    def _try_create_records(self) -> Optional[Sequence[Record]]:
        try:
            return self.create_records()
        except OdbException as e:
            logging.warning("Creating records failed with error [%s]. Will retry.", e)
            logging.debug(traceback.format_exc())
            return None

    def create_records(self) -> Sequence[Record]:
        logging.info("Creating %d records of test data", self.scale)
        recs = [self.select_or_create_record(id) for id in range(1, self.scale + 1)]
        return recs

    def select_or_create_record(self, record_id: int) -> Record:
        rec = self.select_record(record_id)
        if not rec:
            rec = self.create_record(record_id)
        return rec

    def create_record(self, record_id: int) -> Record:
        result = self.odb.cmd(f"INSERT INTO Record SET id = {record_id}, prop_uq = 0, prop_nuq = 0, prop_ftx = 0")["result"]
        return RecordTestDataManager._map_record(result[0])

    def select_record(self, record_id: int) -> Optional[Record]:
        result = self.odb.sql(f"SELECT from Record where id = {record_id}")["result"]
        if result:
            return RecordTestDataManager._map_record(result[0])
        return None

    @staticmethod
    def _map_record(rec: JsonObject) -> Record:
        return Record(rec["@rid"], rec["id"], rec["prop_uq"], rec["prop_nuq"], rec["prop_ftx"])

    def random_record_id(self) -> int:
        assert self.random is not None
        return self.random.randrange(1, self.scale)

    def select_random_record(self) -> Record:
        record_id = self.random_record_id()
        res = self.select_record(record_id)
        if res is None:
            raise OdbException(f"Record {record_id} could not be loaded")
        return res

    def update_record_uq(self, rec: Record) -> int:
        return self.odb.update(f"UPDATE {rec.rid} SET prop_uq = {rec.prop_uq}")

    def update_record_nuq(self, rec: Record) -> int:
        return self.odb.update(f"UPDATE {rec.rid} SET prop_nuq = {rec.prop_nuq}")

    def update_record_ftx(self, rec: Record) -> int:
        return self.odb.update(f"UPDATE {rec.rid} SET prop_ftx = {rec.prop_ftx}")

    def validate_workload(self, workload_validation_readonly: bool) -> Optional[bool]:
        try:
            # TODO: Extract to WorkloadValidator object, track validation count and report formal errors
            # TODO: Do separate validations on each backend server?
            # TODO: Do separate validations for each transaction type
            rec = self.select_or_create_record(self.validation_record_id)
            if rec is None:
                logging.error("Failed to create validation record %s", self.validation_record_id)
                return None
            if not workload_validation_readonly:
                rec = rec.next_uq()
                update_count = self.update_record_uq(rec)
                if update_count != 1:
                    logging.error("Failed update to record %s", self.validation_record_id)
                    return None
                rec2 = self.select_record(rec.record_id)
                if rec2 is None:
                    logging.error(
                        "Failed to re-retrieve validation record %s",
                        self.validation_record_id,
                    )
                    return None

                if rec.prop_uq != rec2.prop_uq:
                    logging.error("Mismatch on record %s. Probable lost update!", rec.record_id)
                    logging.debug(" Updated: %s", rec)
                    logging.debug(" Retrieved: %s", rec2)
                    return None
            return True
        except OdbRestException as e:
            logging.error(
                "Encountered exception during validation: %s: %s %s",
                e.server_root,
                e.code,
                e.message,
            )
            return None
        except OdbException as e:
            logging.error("Encountered exception during validation: %s ", e)
            return None


class RecordTestDataWorkload(FirmThread[int]):
    def __init__(
        self,
        name: str,
        tdm: RecordTestDataManager,
        error_reporter: Scenario.ErrorReporter,
        workload_rate: int,
        workload_readonly: bool,
    ) -> None:
        super().__init__(name=f"RecordTestDataWorkload-{name}")
        self.tdm = tdm
        self.error_reporter = error_reporter
        self.workload_rate = workload_rate
        self.workload_readonly = workload_readonly
        self.request_id = 0
        self.workload_failed = False

    def is_workload_failed(self) -> bool:
        with self.lock:
            return self.workload_failed

    def _fail_workload(self) -> None:
        with self.lock:
            self.workload_failed = True

    def _locked_prepare_work(self) -> int:
        self.request_id += 1
        return self.tdm.random_record_id()

    def _do_work(self, work: int) -> None:
        rec_id = work
        logging.debug("Running workload")

        def query_with_retry() -> Optional[Union[bool, Record]]:
            if not self.is_running():
                # Early exit when thread is stopping
                logging.debug("Aborting workload execution due to thread exit")
                return False
            try:
                rec = None
                if self.workload_readonly:
                    # Balance load on servers so it's somewhat equivalent in load to update path
                    rec = self.tdm.select_record(rec_id)
                    rec = self.tdm.select_record(rec_id)
                    rec = self.tdm.select_record(rec_id)
                else:
                    rec = self._update_record(rec_id)
                return rec
            except OdbRestException as e:
                logging.debug("Query for id %s failed with code %d - will retry", rec_id, e.code)
                self.error_reporter.report_error(self.request_id, f"HTTP {e.server_root} {e.code} {e.message}")
                return None

        result = timed.try_until(query_with_retry, 60)
        if not result:
            logging.warning("Workload queries failed after 60s of retries")
            self._fail_workload()
        else:
            base_sleep = 1.0 / self.workload_rate
            pause_time = random.uniform(base_sleep * 0.5, base_sleep * 1.5)
            self._wait(pause_time)
        logging.debug("Workload batch completed")

    def _update_record(self, record_id: int) -> Optional[Record]:
        rec = self.tdm.select_record(record_id)
        if rec is None:
            logging.error("Failed to load workload record %s", record_id)
            return None
        rec = rec.next_nuq()
        update_count = self.tdm.update_record_nuq(rec)
        if update_count != 1:
            logging.error("Failed update to workload record %s", rec.record_id)
            return None
        rec2 = self.tdm.select_record(rec.record_id)
        if rec2 is None:
            logging.error("Failed to re-retrieve updated workload record %s", rec.record_id)
            return None

        if rec.prop_uq != rec2.prop_uq:
            logging.error("Mismatch on updated workload record %s. Probable lost update!", rec.record_id)
            logging.debug(" Updated: %s", rec)
            logging.debug(" Retrieved: %s", rec2)
            return None
        return rec


class OrientDBErrorClassifier(AbstractErrorClassifier):

    KNOWN_ERRORS = [
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "TCP_IP_CONNECT_REFUSED",
            AbstractErrorClassifier._exc_regex("Connection refused to address.*\\[TcpIpConnector\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "TCP_IP_CONNECT_RESET",
            AbstractErrorClassifier._exc_regex("\\[TcpIpConnection\\].*IOException: Connection reset by peer"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "TCP_IP_CONNECT_ERROR",
            AbstractErrorClassifier._exc_regex(
                "Removing connection to endpoint.*Connection refused to address.*\\[TcpIpConnectionErrorHandler\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "TCP_IP_CONNECT_UNKNOWN_HOST",
            AbstractErrorClassifier._exc_regex(
                "Removing connection to endpoint.*Cause => java.net.UnknownHostException.*\\[TcpIpConnectionErrorHandler\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "HAZ_PARTITION_NOT_MEMBER",
            AbstractErrorClassifier._exc_regex("CallerNotMemberException: Not Member.*\\[PartitionStateOperation\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "HAZ_MIGRATION_FAILED",
            AbstractErrorClassifier._exc_regex("WARNI.*Migration failed.*\\[MigrationManager\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_ASSIGN_NODE_NAME",
            AbstractErrorClassifier._exc_regex("WARNI.*Assigning distributed node name.*\\[OHazelcastPlugin\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_MEMBER_ADDR",
            AbstractErrorClassifier._exc_regex("WARNI.*You configured your member address as host name.*\\[AddressPicker\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_CONFIG_VALIDATOR",
            AbstractErrorClassifier._exc_regex("WARNI.*Property hazelcast.* is deprecated.*\\[ConfigValidator\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_CONFIG_VALIDATOR",
            AbstractErrorClassifier._exc_regex(
                "WARNI.*Error on retrieving 'registeredNodes' from cluster configuration.*\\[OHazelcastPlugin\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_RETRIEVE_NODES",
            AbstractErrorClassifier._exc_regex(
                "WARNI.*Error on retrieving 'registeredNodes' from cluster configuration.*\\[OHazelcastPlugin\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_CONFIG_REPAIR",
            AbstractErrorClassifier._exc_regex("WARNI.*Repairing of 'registeredNodes' completed.*\\[OHazelcastPlugin\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_BACKUP_DB_MOVE",
            AbstractErrorClassifier._exc_regex(
                "WARNI.*Moving existent database.*and get a fresh copy from a remote node.*\\[OHazelcastPlugin\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_BACKUP_OP_UNSUPPORTED",
            AbstractErrorClassifier._exc_regex("SEVER.*not supported during database backup.*\\[OHazelcastPlugin\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_CONNECT_ERROR",
            AbstractErrorClassifier._exc_regex("SEVER.*Error on connecting to node.*\\[OHazelcastPlugin\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_NODE_REMOVED",
            AbstractErrorClassifier._exc_regex("WARNI.*Node removed id=Member.*\\[OHazelcastPlugin\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_CONSEC_ERRS",
            AbstractErrorClassifier._exc_regex(
                "WARNI.*Reached .+ consecutive errors on connection, remove the server.*\\[ORemoteServerChannel\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_SUSPECT_NODE",
            AbstractErrorClassifier._exc_regex("WARNI.*Member .* is suspected to be dead for reason.*\\[MembershipManager\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_DELTA_SYNC",
            AbstractErrorClassifier._exc_regex(
                "WARNI.*requesting delta database sync for .* on local server.*\\[OHazelcastPlugin\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_SHUTDOWN",
            AbstractErrorClassifier._exc_regex("WARNI.*Shutting down node.*\\[OHazelcastPlugin\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "DIST_SHUTDOWN",
            AbstractErrorClassifier._exc_regex("WARNI.*Mastership of .+ is accepted.*\\[ClusterService\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "SRV_SCRIPT_ENABLED",
            AbstractErrorClassifier._exc_regex(
                "WARNI.*Authenticated clients can execute any kind of code into the server.*\\[OServerSideScriptInterpreter\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "RECEIVED_SIGNAL",
            AbstractErrorClassifier._exc_regex("WARNI Received signal.*\\[OSignalHandler\\]"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "REMOTE_CONNECT_REFUSED",
            AbstractErrorClassifier._exc_regex(
                "Cannot determine protocol version for server.*Connection refused.*\\[ORemoteTaskFactoryManagerImpl\\]"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "REMOTE_CONNECT_UNKNOWN_HOST",
            AbstractErrorClassifier._exc_regex(
                "Cannot determine protocol version for server.*\\[ORemoteTaskFactoryManagerImpl\\].*UnknownHostException"
            ),
        ),
        (
            ScenarioError.ErrorClassification.KNOWN,
            "OFFLINE_NO_AUTH_DB",
            AbstractErrorClassifier._exc_regex("OOfflineNodeException.*not online.*executeNoAuthorization"),
        ),
        (
            ScenarioError.ErrorClassification.KNOWN,
            "OFFLINE_NO_AUTH_NODE",
            AbstractErrorClassifier._exc_regex(
                "OOfflineNodeException.*Distributed server is not yet ONLINE.*executeNoAuthorization"
            ),
        ),
        (
            ScenarioError.ErrorClassification.KNOWN,
            "OFFLINE_PROC_DB",
            AbstractErrorClassifier._exc_regex("OOfflineNodeException.*not online.*processRequest"),
        ),
        (
            ScenarioError.ErrorClassification.KNOWN,
            "OFFLINE_PROC_NODE",
            AbstractErrorClassifier._exc_regex("OOfflineNodeException.*Distributed server is not yet ONLINE.*processRequest"),
        ),
        (
            ScenarioError.ErrorClassification.KNOWN,
            "STORAGE_INTERRUPT",
            AbstractErrorClassifier._exc_regex("WARNI Execution  of thread .* is interrupted.*\\[OLocalPaginatedStorage\\]"),
        ),
    ]

    UNKNOWN_ERROR_NAME_PATTERNS = [
        re.compile("(\\w+Exception)"),
        re.compile(" (WARNI|ERRO|SEVER) .*\\[(.+)\\]$"),
    ]

    def __init__(self) -> None:
        super().__init__(
            OrientDBErrorClassifier.KNOWN_ERRORS,
            OrientDBErrorClassifier.UNKNOWN_ERROR_NAME_PATTERNS,
        )


class OrientDBRESTErrorClassifier(AbstractErrorClassifier):

    KNOWN_ERRORS = [
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "HTTP_503",
            AbstractErrorClassifier._exc_regex("HTTP.*503"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "HTTP_500_AVAIL_NODES",
            AbstractErrorClassifier._exc_regex(
                "HTTP.*500.*ODistributedException: Not enough nodes online to execute the operation"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "HTTP_500_QUORUM_FAIL",
            AbstractErrorClassifier._exc_regex("HTTP.*500.*ODistributedOperationException: Request.*didn't reach the quorum of"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "HTTP_500_DIST_LOCK_TIMEOUT",
            AbstractErrorClassifier._exc_regex(
                "HTTP.*500.*ODistributedRecordLockedException: Timeout.*on acquiring lock on record.*on server"
            ),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "HTTP_409",
            AbstractErrorClassifier._exc_regex("HTTP.*409"),
        ),
        (
            ScenarioError.ErrorClassification.SUPPRESSED,
            "HTTP_401",
            AbstractErrorClassifier._exc_regex("HTTP.*401.*Unauthorized"),
        ),
    ]

    UNKNOWN_ERROR_NAME_PATTERNS = [
        re.compile(" (\\d+).*?(\\w+Exception)"),
        re.compile("HTTP (50\\d) "),
        re.compile("HTTP (40\\d) "),
    ]

    def __init__(self) -> None:
        super().__init__(
            OrientDBRESTErrorClassifier.KNOWN_ERRORS,
            OrientDBRESTErrorClassifier.UNKNOWN_ERROR_NAME_PATTERNS,
        )


class RecordTestDataWorkloadManager(ScenarioAware, ScenarioValidator):
    def __init__(
        self,
        scenario: Scenario,
        tdm: RecordTestDataManager,
        workload_threads: int = 1,
        workload_rate: int = 10,
        workload_readonly: bool = False,
        workload_validation_readonly: bool = False,
        **kwargs: Any
    ) -> None:
        self.tdm = tdm
        self.workloads = [
            RecordTestDataWorkload(
                f"workload-{index}",
                tdm,
                scenario.error_reporter(f"workload-{index}", error_classifier=OrientDBRESTErrorClassifier()),
                workload_rate,
                workload_readonly,
            )
            for index in range(1, workload_threads + 1)
        ]
        self.workload_rate = workload_rate
        self.workload_validation_readonly = workload_validation_readonly

    def start(self) -> None:
        logging.info(
            "Starting scenario query workload on %d threads, %0.2fs ops/s",
            len(self.workloads),
            self.workload_rate,
        )
        for wl in self.workloads:
            wl.start()

    def stop(self) -> None:
        def stop_workload(wl: RecordTestDataWorkload, time: float) -> Optional[bool]:
            wl.join(time)
            return None if wl.is_alive() else True

        logging.info("Stopping scenario query workload")
        for wl in self.workloads:
            wl.signal_stop()
        stopped = timed.try_each_timed_until(self.workloads, stop_workload, 10, partial_completion=True)
        assert stopped is not None
        if len(stopped) != len(self.workloads):
            logging.warning("Workload threads did not exit in timely manner")
        logging.info("Scenario query workload stopped")

    def is_workload_failed(self) -> bool:
        return any(wl.is_workload_failed() for wl in self.workloads)

    def on_scenario_begin(self, scenario: Scenario) -> None:
        self.start()

    def on_scenario_end(self, scenario: Scenario) -> None:
        self.stop()

    def validate(self, timeout: float) -> Optional[Any]:
        if self.is_workload_failed():
            # TODO: Could do this with FATAL level in error reporter?
            logging.warning("Background workloads reported failure.")
            return None
        logging.info("Validating availability for data query/update")
        return self.tdm.validate_workload(self.workload_validation_readonly)
