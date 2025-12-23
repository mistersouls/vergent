from dataclasses import dataclass


@dataclass
class BackoffRetry:
    initial: float = 0.5
    maximum: float = 30.0
    factor: float = 2.0
    jitter: float = 1.2

    _current: float = None

    def __post_init__(self):
        self._current = self.initial

    def next_delay(self) -> float:
        delay = self._current

        self._current = min(self._current * self.factor, self.maximum)

        if self.jitter > 0:
            import random
            delay += random.uniform(0, self.jitter)

        return delay

    def reset(self):
        self._current = self.initial
