import logging
import signal
import time
from abc import ABC, abstractmethod
from typing import Any

from orientdb_stress.orientdb import OdbServer
from orientdb_stress.process import OrientDBServerPoolManager, OrientDBServerProcessManager


class AbstractServerSelector(ABC):
    def __init__(self, spm: OrientDBServerPoolManager, **kwargs: Any) -> None:
        self.spm = spm
        self.current_srv = spm.server_pool.choose_last_server(include_not_running=True)

    def choose_next_server(self) -> OdbServer:
        self.current_srv = self._choose_next_server()
        return self.current_srv

    @abstractmethod
    def _choose_next_server(self) -> OdbServer:
        pass


class SequentialServerSelector(AbstractServerSelector):
    def __init__(self, spm: OrientDBServerPoolManager, **kwargs: Any):
        super().__init__(spm, **kwargs)

    def _choose_next_server(self) -> OdbServer:
        return self.spm.server_pool.choose_next_server(self.current_srv)


class RandomServerSelector(AbstractServerSelector):
    def __init__(self, spm: OrientDBServerPoolManager, **kwargs: Any) -> None:
        super().__init__(spm, **kwargs)

    def _choose_next_server(self) -> OdbServer:
        return self.spm.server_pool.choose_random_server_not(self.current_srv)


class AbstractServerRestarter(ABC):
    def __init__(self, spm: OrientDBServerPoolManager, **kwargs: Any):  # pylint: disable=unused-argument
        self.spm = spm

    @abstractmethod
    def restart_server(self, server: OdbServer) -> bool:
        pass


class RestartingServerRestarter(AbstractServerRestarter):
    def __init__(self, spm: OrientDBServerPoolManager, **kwargs: Any) -> None:
        super().__init__(spm, **kwargs)

    def restart_server(self, server: OdbServer) -> bool:
        mgr = self.spm.mgr_for(server.name)
        mgr.restart()
        return True


class ServerStopper(ABC):
    def __init__(self, stop_start_reset_database: bool = False, **kwargs: Any):
        self.stop_start_reset_database = stop_start_reset_database

    def stop(self, mgr: OrientDBServerProcessManager):
        self._stop(mgr)
        if self.stop_start_reset_database:
            mgr.clean_data()

    @staticmethod
    def stopper_for(**kwargs: Any) -> "ServerStopper":
        if kwargs.get("stop_start_kill_server"):
            return KillingServerStopper(**kwargs)
        else:
            return StoppingServerStopper(**kwargs)

    @abstractmethod
    def _stop(self, mgr: OrientDBServerProcessManager):
        pass


class StoppingServerStopper(ServerStopper):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def _stop(self, mgr: OrientDBServerProcessManager) -> None:
        mgr.stop()


class KillingServerStopper(ServerStopper):
    # TODO: Command line args for signal
    def __init__(self, stop_signal: signal.Signals = signal.SIGKILL, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.signal = stop_signal

    def _stop(self, mgr: OrientDBServerProcessManager):
        mgr.kill(self.signal)


class StopStartServerRestarter(AbstractServerRestarter):
    def __init__(self, spm: OrientDBServerPoolManager, dead_time: float = 0, **kwargs: Any) -> None:
        super().__init__(spm, **kwargs)
        self._stopper = ServerStopper.stopper_for(**kwargs)
        self.dead_time = dead_time

    def restart_server(self, server: OdbServer) -> bool:
        mgr = self.spm.mgr_for(server.name)
        self._stopper.stop(mgr)
        logging.debug("Waiting for restart for %0.2fs", self.dead_time)
        time.sleep(self.dead_time)
        mgr.start()
        return True


class AlternatingStopStartServerRestarter(AbstractServerRestarter):
    def __init__(
        self,
        spm: OrientDBServerPoolManager,
        **kwargs: Any,
    ) -> None:
        super().__init__(spm, **kwargs)
        self._stopper = ServerStopper.stopper_for(**kwargs)
        self.stop_phase = True

    def restart_server(self, server: OdbServer):
        mgr = self.spm.mgr_for(server.name)
        if self.stop_phase:
            self.stop_phase = False
            self._stopper.stop(mgr)
        else:
            self.stop_phase = True
            mgr.start()
        return self.stop_phase
