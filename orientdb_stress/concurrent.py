import logging
import threading
from typing import Generic, Optional, TypeVar

W = TypeVar("W")


class FirmThread(threading.Thread, Generic[W]):
    def __init__(self, name: str) -> None:
        super().__init__(name=name, daemon=True)
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.stopped = threading.Event()

    def _wait(self, timeout: float) -> bool:
        with self.condition:
            if self.stopped.is_set():
                return True
            return self.condition.wait(timeout)

    def wait_for_exit(self, timeout: float) -> bool:
        return self.stopped.wait(timeout)

    def _notify(self) -> None:
        with self.condition:
            self.condition.notify_all()

    def is_running(self) -> bool:
        return not self.stopped.is_set()

    def run(self) -> None:
        logging.debug("Thread %s starting", self.name)
        while True:
            work = self._prepare_work()
            if not work:
                break
            self._do_work(work)
        self._on_terminate()

    def _prepare_work(self) -> Optional[W]:
        with self.lock:
            if self.stopped.is_set():
                return None
            return self._locked_prepare_work()

    def _locked_prepare_work(self) -> Optional[W]:
        pass

    def _do_work(self, work: W) -> None:
        pass

    def stop(self) -> None:
        logging.debug("Stopping thread %s", self.name)
        self.signal_stop()
        logging.debug("Stop joining thread %s", self.name)
        if self.is_alive():
            self.join()

    def signal_stop(self) -> None:
        with self.lock:
            self.stopped.set()
            self._locked_signal_stop()
            self.condition.notify_all()

    def _locked_signal_stop(self) -> None:
        pass

    def _on_terminate(self) -> None:
        logging.debug("Thread %s terminating", self.name)
        with self.lock:
            self.stopped.set()
            self.condition.notify_all()
            self._locked_on_terminate()

    def _locked_on_terminate(self) -> None:
        pass
