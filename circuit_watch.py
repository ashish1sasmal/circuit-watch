from datetime import datetime
import time
from enum import Enum
from collections import deque
import random
from typing import List

class CircuitState(Enum):
    OPEN_STATE = "OPEN_STATE"
    CLOSED_STATE = "CLOSED_STATE"
    HALF_OPEN_STATE = "HALF_OPEN_STATE"


class SlidingWindowType(Enum):
    COUNT_BASED = "COUNT_BASED"
    TIME_BASED = "TIME_BASED"


class CBConstants(object):
    DEFAULT_SLIDING_WINDOW_TYPE = SlidingWindowType.COUNT_BASED.value
    DEFAULT_COUNT_BASED_WINDOW_SIZE = 2
    DEFAULT_TIME_BASED_WINDOW_SIZE = 60
    DEFAULT_FAILURE_RATE = 1.0
    DEFAULT_OPEN_STATE_DURATION = 10.0
    DEFAULT_HALF_OPEN_STATE_CALLS = 1
    DEFAULT_HALF_OPEN_STATE_DURATION = 0


class CallDetail(object):
    def __init__(self, status: bool):
        self.status = status
        self.timestamp = datetime.utcnow()


class CircuitStorage(object):
    def __init__(
        self,
        sliding_window_type: SlidingWindowType = None,
        count_based_window_size: int = None,
        time_based_window_size: int = None,
        failure_rate: float = None,
        slow_call_duration: float = None,
        half_open_state_calls_threshold: int = None,
        half_open_state_duration: float = None,
        open_state_duration: float = None,
    ):
        self.sliding_window_type = sliding_window_type or CBConstants.DEFAULT_SLIDING_WINDOW_TYPE
        self.count_based_window_size = count_based_window_size or CBConstants.DEFAULT_COUNT_BASED_WINDOW_SIZE
        self.time_based_window_size = time_based_window_size or CBConstants.DEFAULT_TIME_BASED_WINDOW_SIZE
        self.failure_rate = failure_rate or CBConstants.DEFAULT_FAILURE_RATE
        self.slow_call_duration = slow_call_duration
        self.open_state_duration = open_state_duration or CBConstants.DEFAULT_OPEN_STATE_DURATION
        self.half_open_state_calls_threshold = half_open_state_calls_threshold or CBConstants.DEFAULT_HALF_OPEN_STATE_CALLS
        self.half_open_state_duration = half_open_state_duration or CBConstants.DEFAULT_HALF_OPEN_STATE_DURATION
        self.total_calls = deque()
        self.window_start_time = None
        self.half_open_state_total_calls = 0
        self.circuit_state = CircuitState.CLOSED_STATE


class CircuitBreaker(object):
    def __init__(
        self,
        sliding_window_type: SlidingWindowType = None,
        count_based_window_size: int = None,
        time_based_window_size: int = None,
        failure_rate: float = None,
        slow_call_duration: float = None,
        half_open_state_calls_threshold: int = None,
        half_open_state_duration: float = None,
        open_state_duration: float = None,
        listeners: list = []
    ):
        self.last_failure_time = None
        self.listeners = listeners
        self._storage_state = CircuitStorage(
            sliding_window_type=sliding_window_type,
            count_based_window_size=count_based_window_size,
            time_based_window_size=time_based_window_size,
            failure_rate=failure_rate,
            slow_call_duration=slow_call_duration,
            half_open_state_calls_threshold=half_open_state_calls_threshold,
            half_open_state_duration=half_open_state_duration,
            open_state_duration=open_state_duration,
        )

    def reset_calls(self):
        self._storage_state.total_calls.clear()

    def set_circuit_breaker_state(self, state: CircuitState):
        prev_state = self._storage_state.circuit_state
        self._storage_state.circuit_state = state
        if self.listeners:
            for listener in self.listeners:
                listener.state_change(self, prev_state, state)

    def update_last_failure_time(self):
        self.last_failure_time = datetime.utcnow()

    def update_circuit_state(self, updated_circuit_state: CircuitState):
        self._storage_state.circuit_state = updated_circuit_state

    def check_if_max_fails_reached(self):
        fail_counter = 0
        total_calls = len(self._storage_state.total_calls)
        max_fails_reached = total_calls >= self._storage_state.count_based_window_size
        if max_fails_reached:
            for call in self._storage_state.total_calls:
                fail_counter += 0 if call.status else 1
            max_fails_reached = (
                fail_counter / total_calls
            ) >= self._storage_state.failure_rate
        return max_fails_reached

    def check_if_open_state_duration_elapsed(self):
        return (
            datetime.utcnow() - self.last_failure_time
        ).seconds >= self._storage_state.open_state_duration

    def update_total_calls_details(self):
        while (
            len(self._storage_state.total_calls)
            > self._storage_state.count_based_window_size
        ):
            self._storage_state.total_calls.popleft()

    def add_call_detail(self, status: bool):
        self._storage_state.total_calls.append(CallDetail(status))
        self.update_total_calls_details()

    def check_if_execution_time_breached(self, start_time, end_time):
        if self._storage_state.slow_call_duration is not None:
            if (end_time - start_time) >= self._storage_state.slow_call_duration:
                raise CircuitExecutionTimeBreachedException

    def increment_half_open_state_success_calls(self):
        self._storage_state.half_open_state_total_calls += 1

    def reset_half_open_state_calls_counter(self):
        self._storage_state.half_open_state_total_calls = 0

    def check_half_open_call_success(self):
        return (
            self._storage_state.half_open_state_total_calls
            >= self._storage_state.half_open_state_calls_threshold
        )

    def get_circuit_state(self):
        return {
            CircuitState.OPEN_STATE: CircuitOpenState,
            CircuitState.CLOSED_STATE: CircuitCloseState,
            CircuitState.HALF_OPEN_STATE: CircuitHalfOpenState,
        }.get(self._storage_state.circuit_state)

    def handle_before_call(self):
        circuit_state = self.get_circuit_state()(self)
        circuit_state.before_call()

    def handle_success(self):
        circuit_state = self.get_circuit_state()(self)
        circuit_state.on_success()

    def handle_failure(self, exc: Exception):
        circuit_state = self.get_circuit_state()(self)
        circuit_state.on_failure(exc)

    def circuit_close(self):
        self._storage_state.circuit_state = CircuitState.CLOSED_STATE

    def circuit_open(self):
        self._storage_state.circuit_state = CircuitState.OPEN_STATE
    
    def circuit_half_open(self):
        self._storage_state.circuit_state = CircuitState.HALF_OPEN_STATE

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            self.handle_before_call()
            try:
                start_time = time.time()
                result = func(*args, **kwargs)
                self.check_if_execution_time_breached(start_time, time.time())
                self.handle_success()
                return result
            except Exception as cbe:
                self.handle_failure(cbe)
                raise

        return wrapper


class CircuitBreakerState(object):
    def __init__(self, cb: CircuitBreaker):
        self.circuit_breaker = cb

    def on_success(self):
        self.circuit_breaker.add_call_detail(True)
        if self.circuit_breaker.listeners:
            for listener in self.circuit_breaker.listeners:
                listener.success(self.circuit_breaker)

    def on_failure(self, exc: Exception):
        if self.circuit_breaker.listeners:
            for listener in self.circuit_breaker.listeners:
                listener.failure(self.circuit_breaker, exc)

    def before_call(self):
        pass


class CircuitCloseState(CircuitBreakerState):
    def __init__(self, cb: CircuitBreaker):
        super(CircuitCloseState, self).__init__(cb)

    def on_failure(self, exc:Exception):
        self.circuit_breaker.add_call_detail(False)
        self.circuit_breaker.update_last_failure_time()
        if self.circuit_breaker.check_if_max_fails_reached():
            self.circuit_breaker.set_circuit_breaker_state(CircuitState.OPEN_STATE)
        super(CircuitCloseState, self).on_failure(exc)


class CircuitOpenState(CircuitBreakerState):
    def __init__(self, cb: CircuitBreaker):
        super(CircuitOpenState, self).__init__(cb)

    def on_failure(self, exc: Exception):
        pass

    def before_call(self):
        if self.circuit_breaker.check_if_open_state_duration_elapsed():
            self.circuit_breaker.set_circuit_breaker_state(CircuitState.HALF_OPEN_STATE)
        else:
            raise CircuitOpenException


class CircuitHalfOpenState(CircuitBreakerState):
    def __init__(self, cb: CircuitBreaker):
        super(CircuitHalfOpenState, self).__init__(cb)

    def on_success(self):
        self.circuit_breaker.increment_half_open_state_success_calls()
        if self.circuit_breaker.check_half_open_call_success():
            self.circuit_breaker.set_circuit_breaker_state(CircuitState.CLOSED_STATE)
            self.circuit_breaker.reset_calls()
            self.circuit_breaker.reset_half_open_state_calls_counter()

    def on_failure(self, exc: Exception):
        self.circuit_breaker.set_circuit_breaker_state(CircuitState.OPEN_STATE)
        self.circuit_breaker.update_last_failure_time()
        self.circuit_breaker.reset_half_open_state_calls_counter()

    def before_call(self):
        pass

class CircuitBreakerListener(object):
    def success(self, cb: CircuitBreaker):
        pass
    
    def on_failure(self, cb: CircuitBreaker, exc: Exception):
        pass

    def before_call(self, cb: CircuitBreaker, func, *args, **kwargs):
        pass

    def state_change(self, cb: CircuitBreaker, prev_state: CircuitBreakerState, curr_state: CircuitBreakerState):
        pass

class CircuitOpenException(Exception):
    def __init__(self):
        super().__init__("Reset time not elapsed yet. Circuit in open state.")


class CircuitExecutionTimeBreachedException(Exception):
    def __init__(self):
        super().__init__("Method execution has breached threshold time.")


@CircuitBreaker(sliding_window_type=SlidingWindowType.COUNT_BASED, count_based_window_size=10, failure_rate=0.5)
def check(i, error=False):
    print(i, error)
    if error:
        print(9 / 0)


for i in range(30):
    try:
        check(i, i>=5 and i<10 or i>13 and i<=14)
    except CircuitOpenException as err:
        print(str(err))
    except Exception as err:
        pass
    if i==13 or i==15:
        time.sleep(2)
