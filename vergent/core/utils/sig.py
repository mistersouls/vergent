import contextlib
import functools
import sys
import signal
import threading
from collections.abc import Callable
from types import FrameType
from typing import Generator

SHUTDOWN_SIGNALS = (
    signal.SIGINT,
    signal.SIGTERM,
)

if sys.platform == "win32":
    SHUTDOWN_SIGNALS += (signal.SIGBREAK,)


@contextlib.contextmanager
def signal_handler(handler: Callable[[signal.Signals | int, FrameType], None]) -> Generator[None, None, None]:
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    captured_signals: list[signal.Signals | int] = []

    @functools.wraps(handler)
    def capture(sig: int, frame: FrameType) -> None:
        captured_signals.append(sig)
        handler(sig, frame)

    # Install temporary handlers
    original_handlers = {
        sig: signal.signal(sig, capture)
        for sig in SHUTDOWN_SIGNALS
    }

    try:
        yield
    finally:
        # Restore original handlers
        for sig, old in original_handlers.items():
            signal.signal(sig, old)

        # Now replay signals with the real handler
        for sig in reversed(captured_signals):
            if original_handlers[sig] is not handler:
                signal.raise_signal(sig)
