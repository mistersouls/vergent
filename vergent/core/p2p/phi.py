import time
import math
from collections import deque


class FailureDetector:
    """
    φ-accrual failure detector (Hayashibara et al. 2004).

    This implementation follows the standard model:

        φ(t) = -log10( P(T > t) )

    where T is modeled as an exponential distribution whose mean is
    estimated from the sliding window of inter-arrival times.

    Notes:
    - The exponential survival function is S(t) = exp(-t / mean).
    - Floating-point underflow is handled by clamping S(t) to a minimum
      positive value, as done in Cassandra and Akka.
    """

    def __init__(self, threshold: float = 2.0, window_size: int = 50):
        self.threshold = threshold
        self.arrival_times = deque(maxlen=window_size)

    def record_heartbeat(self, timestamp: float | None = None) -> None:
        """
        Record the arrival time of a heartbeat.
        """
        self.arrival_times.append(timestamp or time.time())

    def compute_phi(self, now: float | None = None) -> float:
        """
        Compute φ(t) = -log10( survival_function(t) ).

        The survival function for an exponential distribution is:

            S(t) = exp(-t / mean)

        Numerical stability:
        - If exp(-t/mean) underflows to 0.0, we clamp it to a small
          positive value (1e-15). This is standard practice in
          Cassandra, Akka, and Riak to avoid log10(0).
        """
        if not self.arrival_times:
            # No heartbeat ever received → infinite suspicion
            return float("inf")

        if now is None:
            now = time.time()

        times = list(self.arrival_times)
        inter_arrivals = [t2 - t1 for t1, t2 in zip(times, times[1:])]

        if not inter_arrivals:
            # Only one heartbeat → insufficient data
            return 0.0

        # Mean inter-arrival time
        mean = sum(inter_arrivals) / len(inter_arrivals)
        mean = max(mean, 1e-6)  # avoid division by zero

        # Time since last heartbeat
        last = self.arrival_times[-1]
        delta = now - last

        if delta <= 0:
            # Clock skew or immediate heartbeat → no suspicion
            return 0.0

        # Exponential survival function
        x = -delta / mean

        # Clamp to avoid underflow: exp(-50) ≈ 1.9e-22 → φ ≈ 21.7
        # Beyond this, φ is already "very suspicious".
        if x < -50:
            return 50.0  # cap φ to a large value

        survival = math.exp(x)

        # Clamp survival to avoid log10(0)
        survival = max(survival, 1e-15)

        return -math.log10(survival)

    def is_suspect(self, now: float | None = None) -> bool:
        """
        Return True if φ exceeds the configured threshold.
        """
        return self.compute_phi(now) >= self.threshold

    def reset(self) -> None:
        """
        Clear all recorded heartbeats (e.g., after recovery).
        """
        self.arrival_times.clear()
