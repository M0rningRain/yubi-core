"""Wait for the Jazzy recorder's own ready message, forwarding its output."""

import os
import subprocess
import sys
import threading
import time


def start_recorder(command, timeout_sec=10.0):
    """Return (process, ready) only after Recording... or a bounded failure.

    Jazzy emits Recording... after opening the writer and creating subscriptions.
    Keep draining output after readiness, so a full pipe cannot stall recording.
    """
    env = os.environ.copy()
    env.update(
        RCUTILS_CONSOLE_OUTPUT_FORMAT="[{name}] {message}",
        RCUTILS_COLORIZED_OUTPUT="0",
        RCUTILS_LOGGING_BUFFERED_STREAM="0",
    )
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1, env=env,
    )
    ready = threading.Event()
    finished = threading.Event()

    def forward_output():
        try:
            with process.stdout:
                for line in process.stdout:
                    if line.strip() == "[rosbag2_recorder] Recording...":
                        ready.set()
                    try:
                        sys.stdout.write(line)
                        sys.stdout.flush()
                    except OSError:
                        # Losing the console must not stop draining the recorder.
                        pass
        finally:
            finished.set()

    threading.Thread(target=forward_output, daemon=True).start()
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if process.poll() is not None or finished.is_set():
            break
        if ready.wait(timeout=min(0.05, max(0, deadline - time.monotonic()))):
            return process, process.poll() is None
    return process, False


def stop_failed_recorder(process):
    """Reap a failed/timed-out startup, including a recorder ignoring SIGTERM."""
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
