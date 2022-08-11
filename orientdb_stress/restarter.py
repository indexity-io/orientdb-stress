import logging
import signal
import time
from abc import ABC, abstractmethod
from typing import Any

from orientdb_stress.orientdb import OdbServer
from orientdb_stress.process import OrientDBServerPoolManager


class AbstractServerRestarter(ABC):
    def __init__(self, spm: OrientDBServerPoolManager, dead_time: float = 0, **kwargs: Any):  # pylint: disable=unused-argument
        self.spm = spm
        self.current_srv = spm.server_pool.choose_last_server(include_not_running=True)
        self.iteration = 0
        self.dead_time = dead_time

    def restart_next(self) -> None:
        self.iteration += 1
        self.current_srv = self._choose_next_server()
        self._restart_server()

    @abstractmethod
    def _choose_next_server(self) -> OdbServer:
        pass

    def _restart_server(self) -> None:
        mgr = self.spm.mgr_for(self.current_srv.name)
        if self.dead_time > 0.01:
            mgr.stop()
            logging.debug("Waiting for restart for %0.2fs", self.dead_time)
            time.sleep(self.dead_time)
            mgr.start()
        else:
            mgr.restart()


class SequentialServerRestarter(AbstractServerRestarter):
    def __init__(self, spm: OrientDBServerPoolManager, **kwargs: Any) -> None:
        super().__init__(spm, **kwargs)

    def _choose_next_server(self) -> OdbServer:
        return self.spm.server_pool.choose_next_server(self.current_srv)


class AlternatingStopStartServerRestarter(AbstractServerRestarter):
    def __init__(
        self,
        spm: OrientDBServerPoolManager,
        alternating_reset_server: bool = False,
        alternating_kill_server: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(spm, **kwargs)
        self.alternating_reset_server = alternating_reset_server
        self.alternating_kill_server = alternating_kill_server
        self.stop_phase = False

    def _choose_next_server(self) -> OdbServer:
        if self.stop_phase:
            self.stop_phase = False
            return self.current_srv
        self.stop_phase = True
        return self.spm.server_pool.choose_next_server(self.current_srv)

    def _restart_server(self) -> None:
        mgr = self.spm.mgr_for(self.current_srv.name)
        if self.stop_phase:
            if self.alternating_kill_server:
                mgr.kill(signal.SIGKILL)
            else:
                mgr.stop()
        else:
            if self.alternating_reset_server:
                mgr.clean_data()
            mgr.start()


class RandomServerRestarter(AbstractServerRestarter):
    def __init__(self, spm: OrientDBServerPoolManager, **kwargs: Any) -> None:
        super().__init__(spm, **kwargs)

    def _choose_next_server(self) -> OdbServer:
        return self.spm.server_pool.choose_random_server_not(self.current_srv)


class RandomServerKiller(AbstractServerRestarter):
    def __init__(self, spm: OrientDBServerPoolManager, stop_signal: signal.Signals = signal.SIGKILL, **kwargs: Any) -> None:
        super().__init__(spm, **kwargs)
        self.signal = stop_signal

    def _choose_next_server(self) -> OdbServer:
        return self.spm.server_pool.choose_random_server_not(self.current_srv)

    def _restart_server(self) -> None:
        mgr = self.spm.mgr_for(self.current_srv.name)
        mgr.kill(self.signal)
        logging.debug("Waiting for restart for %0.2fs", self.dead_time)
        time.sleep(self.dead_time)
        mgr.start()


# class RollingServerRestart(FirmThread):

# 	def __init__(self, dcm, interval, *args, count = sys.maxsize):
# 		super(RollingServerRestart,self).__init__(name='RollingServerRestart')
# 		self.restarter = SequentialServerRestarter(dcm, *args)
# 		self.interval = interval
# 		self.count = count

# 	def _do_work(self, restarter):
# 		restarter.restart_next()
# 		self._wait(self.interval)

# 	def _locked_prepare_work(self):
# 		if self.restarter.iteration >= self.count:
# 			return None
# 		return self.restarter


# class RandomServerKillerThread(FirmThread):

# 	def __init__(self, dcm, interval, *args, **kwargs):
# 		super(RandomServerKillerThread,self).__init__(name='RandomServerKiller')
# 		self.restarter = RandomServerKiller(dcm, *args, **kwargs)
# 		self.interval = interval

# 	def _do_work(self, restarter):
# 		restarter.restart_next()
# 		self._wait(self.interval)


# 	def _locked_prepare_work(self):
# 		return self.restarter
