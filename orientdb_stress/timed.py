import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, List, Optional, Sequence, TypeVar

T = TypeVar("T")
I = TypeVar("I")

TimedAction = Callable[[float], Optional[T]]
UntimedAction = Callable[[], Optional[T]]


class Timer:
    def __init__(self, timeout: float) -> None:
        self.end = time.time() + timeout

    def is_active(self) -> bool:
        # logging.critical("Timer active: %s", (time.time() < self.end))
        return time.time() < self.end

    def remaining(self) -> float:
        # logging.critical("Time remaining: %d", self.end - time.time())
        return self.end - time.time()

    def invoke_timed_if_active(self, timed_action: TimedAction[T], at_most: Optional[float] = None) -> Optional[T]:
        rem = self.remaining()
        if at_most:
            rem = min(at_most, rem)
        if rem <= 0:
            return None
        return timed_action(rem)

    def invoke_if_active(self, action: UntimedAction[T]) -> Optional[T]:
        return self.invoke_timed_if_active(lambda rem: action())


class TryResult(ABC, Generic[T]):
    def is_success(self) -> bool:
        return False

    def is_timeout(self) -> bool:
        return False

    def is_failure(self) -> bool:
        return False

    @abstractmethod
    def as_try(self) -> Optional[T]:
        pass

    def result(self) -> T:
        res = self.as_try()
        assert res is not None
        return res


class Timeout(TryResult[T]):
    def is_timeout(self) -> bool:
        return True

    def as_try(self) -> Optional[T]:
        return None


class Success(TryResult[T]):
    def __init__(self, result: T) -> None:
        self._result = result

    def is_success(self) -> bool:
        return True

    def as_try(self) -> Optional[T]:
        return self._result


class Failure(TryResult[T]):
    def is_failure(self) -> bool:
        return True

    def as_try(self) -> Optional[T]:
        raise ValueError("Result is failure")


def _untimed(action: UntimedAction[T]) -> TimedAction[T]:
    return lambda rem: action()


def _untimed_per(action: Callable[[I], T]) -> Callable[[I, float], T]:
    return lambda item, rem: action(item)


def boolean_to_try(bool_value: bool) -> Optional[bool]:
    return True if bool_value is True else None


def failing_exception(action: UntimedAction[T]) -> UntimedAction[T]:
    def _fail() -> Optional[T]:
        # noinspection PyBroadException
        try:
            return action()
        except Exception:  # pylint: disable=broad-except
            return None

    return _fail


def try_predicate_until(predicate: Callable[[], bool], timeout: float) -> Optional[bool]:
    return try_until(lambda: boolean_to_try(predicate()), timeout)


def try_exceptional_until(exceptional_action: UntimedAction[T], timeout: float) -> Optional[T]:
    return try_until(failing_exception(exceptional_action), timeout)


def try_until(action: UntimedAction[T], timeout: float) -> Optional[T]:
    return try_timed_until(_untimed(action), timeout)


def try_timed_until(timed_action: TimedAction[T], timeout: float) -> Optional[T]:
    return attempt_timed_until(timed_action, timeout).as_try()


def attempt_timed_until(timed_action: TimedAction[T], timeout: float) -> TryResult[T]:
    timer = Timer(timeout)
    while timer.is_active():
        res = timer.invoke_timed_if_active(timed_action)
        if res is not None:
            return Success(res)
        timer.invoke_timed_if_active(lambda rem: time.sleep(min(rem, 0.5)))
    return Timeout()


def repeat_until(action: UntimedAction[T], timeout: float) -> None:
    repeat_timed_until(_untimed(action), timeout)


def repeat_timed_until(timed_action: TimedAction[T], timeout: float) -> None:
    def _always_incomplete(action: TimedAction[T], action_timeout: float) -> None:
        action(action_timeout)

    try_timed_until(lambda rem: _always_incomplete(timed_action, rem), timeout)


def repeat_timed_until_failure(timed_action: TimedAction[T], timeout: float) -> None:
    def _exit_on_failure(action: TimedAction[T], action_timeout: float) -> Optional[bool]:
        res = action(action_timeout)
        if res is None:
            # Invert action result - allow Try to exit when action fails
            return True
        return None

    try_timed_until(lambda rem: _exit_on_failure(timed_action, rem), timeout)


def try_each_timed_until(
    items: Sequence[I], timed_per_action: Callable[[I, float], Optional[T]], timeout: float, partial_completion: bool = False
) -> Optional[Sequence[T]]:
    timer = Timer(timeout)
    results: List[T] = []
    for item in items:
        res = attempt_timed_until(
            lambda rem: timed_per_action(item, rem), timer.remaining()  # pylint: disable=cell-var-from-loop
        )
        if res.is_timeout():
            if partial_completion:
                return results
            return None
        results.append(res.result())
    return results


def try_each_until(
    items: Sequence[I], per_action: Callable[[I], Optional[T]], timeout: float, **kwargs: Any
) -> Optional[Sequence[T]]:
    return try_each_timed_until(items, _untimed_per(per_action), timeout, **kwargs)


def try_each_predicate_until(
    items: Sequence[I], per_action: Callable[[I], bool], timeout: float, **kwargs: Any
) -> Optional[Sequence[bool]]:
    return try_each_until(items, lambda item: boolean_to_try(per_action(item)), timeout, **kwargs)


def try_each_timed_predicate_until(
    items: Sequence[I], timed_per_action: Callable[[I, float], bool], timeout: float, **kwargs: Any
) -> Optional[Sequence[bool]]:
    return try_each_timed_until(
        items,
        lambda item, rem: boolean_to_try(timed_per_action(item, rem)),
        timeout,
        **kwargs,
    )


def try_all_until(actions: Sequence[UntimedAction[T]], timeout: float, **kwargs: Any) -> Optional[Sequence[T]]:
    return try_each_until(actions, lambda act: act(), timeout, **kwargs)


def try_all_timed_until(timed_actions: Sequence[TimedAction[T]], timeout: float, **kwargs: Any) -> Optional[Sequence[T]]:
    return try_each_timed_until(timed_actions, lambda timed_act, rem: timed_act(rem), timeout, **kwargs)
