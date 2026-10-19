"""Check recorder readiness using real child processes, without ROS/hardware."""

import subprocess
import sys

import pytest

from yubi_core.recording_process import start_recorder, stop_failed_recorder


@pytest.mark.parametrize("output,expected", [
    ("[rosbag2_recorder] Recording...", True),
    ("[rosbag2_recorder] Listening for topics...", False),
    ("[rosbag2_recorder] Wait for recording: Press SPACE to start.", False),
    ("[other] Recording...", False),
])
def test_only_recorder_ready_message_counts(output, expected):
    command = [sys.executable, "-u", "-c",
               f"import time; print({output!r}); time.sleep(5)"]
    process, ready = start_recorder(command, timeout_sec=0.3)
    try:
        assert ready is expected
        assert process.poll() is None
    finally:
        stop_failed_recorder(process)


def test_startup_crash_is_not_recording():
    process, ready = start_recorder(
        [sys.executable, "-c", "raise SystemExit(2)"], timeout_sec=1,
    )
    stop_failed_recorder(process)
    assert ready is False
    assert process.returncode == 2


def test_readiness_does_not_stop_output_drain():
    command = [sys.executable, "-u", "-c", "\n".join([
        "import time",
        "print('[rosbag2_recorder] Recording...')",
        "time.sleep(0.1)",
        "print('x' * 200000)",
    ])]
    process, ready = start_recorder(command, timeout_sec=1)
    assert ready is True
    try:
        # This would hang with a full pipe if the reader stopped on readiness.
        assert process.wait(timeout=2) == 0
    finally:
        stop_failed_recorder(process)


def test_missing_executable():
    with pytest.raises(FileNotFoundError):
        start_recorder(["/does-not-exist/ros2"])


def test_ignoring_termination_is_killed():
    # Use a subprocess rather than waiting two real seconds for the timeout.
    from unittest.mock import MagicMock

    process = MagicMock()
    process.poll.return_value = None
    process.wait.side_effect = [subprocess.TimeoutExpired("recorder", 2), 0]
    stop_failed_recorder(process)
    process.terminate.assert_called_once()
    process.kill.assert_called_once()
