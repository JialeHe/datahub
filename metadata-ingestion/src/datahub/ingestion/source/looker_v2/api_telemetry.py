"""
API Telemetry for Looker V2 Source.

Tracks API call latencies and provides top-K slowest calls reporting.
"""

from __future__ import annotations

import os
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, Generator, Optional

from datahub.utilities.lossy_collections import LossyDict


@dataclass
class LookerAPITelemetry:
    """
    Tracks API call latencies for top-K reporting.

    Configurable via environment variables:
    - DATAHUB_LOOKER_API_TELEMETRY_TOP_K: Number of slowest calls to track (default: 50)
    - DATAHUB_LOOKER_API_TELEMETRY_ENABLED: Enable/disable telemetry (default: true)
    """

    top_k: int = field(
        default_factory=lambda: int(
            os.getenv("DATAHUB_LOOKER_API_TELEMETRY_TOP_K", "50")
        )
    )
    enabled: bool = field(
        default_factory=lambda: os.getenv(
            "DATAHUB_LOOKER_API_TELEMETRY_ENABLED", "true"
        ).lower()
        == "true"
    )

    # Top-K slowest API calls: "method:identifier" -> max duration in seconds
    call_latencies: LossyDict[str, float] = field(default_factory=lambda: LossyDict())

    # Aggregate call counts by method name
    call_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Total time spent on API calls
    total_time: float = 0.0

    # Lock for thread-safe updates
    _lock: Lock = field(default_factory=Lock)

    def __post_init__(self) -> None:
        # Reinitialize LossyDict with correct top_k if needed
        if not isinstance(self.call_latencies, LossyDict):
            self.call_latencies = LossyDict()

    @contextmanager
    def track(
        self, method_name: str, identifier: str = ""
    ) -> Generator[None, None, None]:
        """
        Context manager to track API call duration.

        Args:
            method_name: The API method being called (e.g., "all_dashboards", "dashboard")
            identifier: Optional identifier for the specific call (e.g., dashboard_id)

        Example:
            with telemetry.track("dashboard", dashboard_id):
                result = api.dashboard(dashboard_id)
        """
        if not self.enabled:
            yield
            return

        key = f"{method_name}:{identifier}" if identifier else method_name
        start = time.time()
        try:
            yield
        finally:
            duration = time.time() - start
            with self._lock:
                # Track the maximum duration for this key
                current = self.call_latencies.get(key, 0.0)
                if duration > current:
                    self.call_latencies[key] = duration
                self.call_counts[method_name] += 1
                self.total_time += duration

    def record_call(
        self, method_name: str, duration: float, identifier: str = ""
    ) -> None:
        """
        Record an API call duration directly.

        Args:
            method_name: The API method being called
            duration: Duration in seconds
            identifier: Optional identifier for the specific call
        """
        if not self.enabled:
            return

        key = f"{method_name}:{identifier}" if identifier else method_name
        with self._lock:
            current = self.call_latencies.get(key, 0.0)
            if duration > current:
                self.call_latencies[key] = duration
            self.call_counts[method_name] += 1
            self.total_time += duration

    def _top_k_slowest_unlocked(self) -> Dict[str, float]:
        """Get top-K slowest calls. Caller must hold self._lock."""
        sorted_items = sorted(
            self.call_latencies.items(), key=lambda x: x[1], reverse=True
        )[: self.top_k]
        return dict(sorted_items)

    def get_top_k_slowest(self) -> Dict[str, float]:
        """
        Get the top-K slowest API calls.

        Returns:
            Dictionary mapping "method:identifier" to duration in seconds,
            sorted by duration descending.
        """
        with self._lock:
            return self._top_k_slowest_unlocked()

    def get_summary(self) -> Dict[str, object]:
        """
        Get a summary of API telemetry.

        Returns:
            Dictionary with telemetry summary including:
            - api_call_latencies: Top-K slowest calls
            - api_call_counts: Call counts by method
            - api_total_time_seconds: Total time spent on API calls
        """
        with self._lock:
            return {
                "api_call_latencies": self._top_k_slowest_unlocked(),
                "api_call_counts": dict(self.call_counts),
                "api_total_time_seconds": round(self.total_time, 3),
            }


class LookerRateLimiter:
    """
    Rate limiter for Looker API requests.

    Ensures requests don't exceed a specified rate to avoid hitting API limits.
    """

    def __init__(self, requests_per_second: float = 10.0):
        """
        Initialize the rate limiter.

        Args:
            requests_per_second: Maximum requests per second (default: 10)
        """
        self.min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self.last_request_time: Optional[float] = None
        self._lock = Lock()

    def wait(self) -> None:
        """Wait if necessary to respect the rate limit."""
        if self.min_interval <= 0:
            return

        with self._lock:
            if self.last_request_time is not None:
                elapsed = time.time() - self.last_request_time
                if elapsed < self.min_interval:
                    time.sleep(self.min_interval - elapsed)
            self.last_request_time = time.time()

    @contextmanager
    def limit(self) -> Generator[None, None, None]:
        """Context manager that waits before yielding."""
        self.wait()
        yield
