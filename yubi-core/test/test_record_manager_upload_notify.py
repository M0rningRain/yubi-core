"""Unit tests for RecordManager's upload-notification behaviour on stop_recording.

All ROS2 dependencies are mocked via ``conftest.mock_rclpy``.
"""

import json
import threading
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper: build a RecordManager without hitting __init__ (blocks on services)
# ---------------------------------------------------------------------------


@pytest.fixture()
def record_manager(mock_rclpy, tmp_record_dir):
    """Create a ``RecordManager`` instance bypassing ``__init__``."""
    from yubi_core.record_manager import RecordManager

    # Bypass __init__ which blocks on service discovery
    node = RecordManager.__new__(RecordManager)

    # Manually wire up FakeNode behaviour
    from conftest import FakeNode

    fake = FakeNode("record_manager")
    node._name = fake._name
    node._params = fake._params
    node._logger = fake._logger

    # Satisfy declare_parameter / get_parameter via FakeNode methods
    node.declare_parameter = fake.declare_parameter
    node.get_parameter = fake.get_parameter
    node.get_logger = fake.get_logger
    node.create_publisher = fake.create_publisher
    node.create_timer = fake.create_timer
    node.create_service = fake.create_service
    node.create_client = fake.create_client
    node.destroy_node = fake.destroy_node

    # Instance attributes that __init__ would set
    node.rosbag_process = None
    node.is_recording = False
    node._recording_lock = threading.RLock()
    node.recent_record_dir = None
    node.task_name = None
    node.reent_cg = MagicMock()

    node.record_base_dir = str(tmp_record_dir)
    node.robot_id = "test_robot"
    node.site = "test_lab"
    node.location = ""
    node.record_topics = ["/tf"]
    node.rosbag_params = ["--storage", "mcap"]
    node.qos_overrides_file = ""
    node.required_free_space = 0

    # Publishers
    node.rosbag_manager_mode_pub = MagicMock()
    node.storage_free_space_pub = MagicMock()
    node.storage_used_space_pub = MagicMock()
    node.recording_completed_pub = MagicMock()
    node.recording_feedback_pub = MagicMock()

    # Service clients
    node.init_meta_client = MagicMock()
    node.add_file_client = MagicMock()
    node.extend_entities_client = MagicMock()
    node.extend_components_client = MagicMock()
    node.get_verified_metadata_client = MagicMock()

    return node


# ===================================================================
# TestStopRecordingPublish
# ===================================================================


class TestStopRecordingPublish:
    """Verify that ``stop_recording`` publishes to ``recording_completed``."""

    def test_publishes_on_successful_stop(self, record_manager, tmp_record_dir):
        # Prepare: simulate an active recording
        rec_dir = tmp_record_dir / "task_001"
        rec_dir.mkdir()
        record_manager.is_recording = True
        record_manager.recent_record_dir = str(rec_dir)
        record_manager.task_name = "task_001"
        record_manager.rosbag_process = MagicMock()

        # Mock metadata verification to succeed
        future = MagicMock()
        result = MagicMock()
        result.success = True
        result.message = json.dumps({"task": "test"})
        future.result.return_value = result
        future.done.return_value = True
        record_manager.get_verified_metadata_client.call_async.return_value = future

        success, msg = record_manager.stop_recording()

        assert success is True
        record_manager.recording_completed_pub.publish.assert_called_once()
        call_arg = record_manager.recording_completed_pub.publish.call_args[0][0]
        assert call_arg.data == str(rec_dir)
        feedback = json.loads(record_manager.recording_feedback_pub.publish.call_args.args[0].data)
        assert feedback["state"] == "completed"

    def test_no_publish_on_metadata_failure(self, record_manager, tmp_record_dir):
        rec_dir = tmp_record_dir / "task_002"
        rec_dir.mkdir()
        record_manager.is_recording = True
        record_manager.recent_record_dir = str(rec_dir)
        record_manager.task_name = "task_002"
        record_manager.rosbag_process = MagicMock()

        # Mock metadata verification to fail
        future = MagicMock()
        result = MagicMock()
        result.success = False
        result.message = ""
        future.result.return_value = result
        future.done.return_value = True
        record_manager.get_verified_metadata_client.call_async.return_value = future

        # Patch delete_last_recording to avoid filesystem side effects
        with patch.object(record_manager, "delete_last_recording", return_value=True):
            success, msg = record_manager.stop_recording()

        assert success is False
        record_manager.recording_completed_pub.publish.assert_not_called()
        assert record_manager.rosbag_manager_mode_pub.publish.call_args.args[0].data is False
        assert json.loads(record_manager.recording_feedback_pub.publish.call_args.args[0].data)["state"] == "failed"

    def test_no_publish_when_not_recording(self, record_manager):
        record_manager.is_recording = False
        record_manager.rosbag_process = None

        success, msg = record_manager.stop_recording()

        assert success is False
        record_manager.recording_completed_pub.publish.assert_not_called()


def test_unexpected_exit_publishes_stopped_without_upload(record_manager):
    record_manager.is_recording = True
    record_manager.rosbag_process = MagicMock()
    record_manager.rosbag_process.poll.return_value = 1
    record_manager.publish_recording_state()
    assert not record_manager.is_recording
    assert record_manager.rosbag_process is None
    assert record_manager.rosbag_manager_mode_pub.publish.call_args.args[0].data is False
    record_manager.recording_completed_pub.publish.assert_not_called()
    assert json.loads(record_manager.recording_feedback_pub.publish.call_args.args[0].data)["state"] == "failed"


def test_live_process_keeps_recording_state(record_manager):
    record_manager.is_recording = True
    record_manager.rosbag_process = MagicMock()
    record_manager.rosbag_process.poll.return_value = None
    record_manager.publish_recording_state()
    assert record_manager.rosbag_manager_mode_pub.publish.call_args.args[0].data is True


@pytest.fixture
def ready_manager(record_manager):
    node = record_manager
    for name, value in {
        "robot_type": "yubi", "environment_type": "real_world",
        "teleop_interface": "pico", "teleop_interface_url": "",
        "teleop_interface_hash": "", "teleop_interface_branch": "",
        "devices": "[]",
    }.items():
        node.declare_parameter(name, value)
    node.use_sim_time = False
    node.record_url = node.record_hash = node.record_branch = "test"
    for name in ("set_robot_client", "set_environment_client",
                 "extend_programs_client", "set_devices_client"):
        setattr(node, name, MagicMock())
    return node


@pytest.mark.parametrize("ready", [True, False])
def test_start_status_requires_recorder_readiness(ready_manager, ready):
    node = ready_manager
    process = MagicMock()
    process.poll.return_value = None

    def start(command):
        # A button/service request alone must never publish recording=true.
        assert not node.is_recording
        node.rosbag_manager_mode_pub.publish.assert_not_called()
        assert "--disable-keyboard-controls" in command
        return process, ready

    with patch("yubi_core.record_manager.start_recorder", side_effect=start), \
            patch("yubi_core.record_manager.stop_failed_recorder") as stop:
        success, message = node.start_recording("voice-test")
    assert success is ready
    assert node.is_recording is ready
    assert node.rosbag_manager_mode_pub.publish.call_args.args[0].data is ready
    assert stop.call_count == (0 if ready else 1)
    if ready:
        assert json.loads(node.recording_feedback_pub.publish.call_args.args[0].data) == {
            "state": "recording", "recording_id": "voice-test",
        }
    else:
        node.recording_feedback_pub.publish.assert_not_called()


def test_launch_exception_does_not_publish_active(ready_manager):
    with patch("yubi_core.record_manager.start_recorder", side_effect=OSError("failed")):
        success, _ = ready_manager.start_recording("voice-test")
    assert not success
    assert not ready_manager.is_recording
    ready_manager.rosbag_manager_mode_pub.publish.assert_not_called()


def test_stop_status_only_changes_after_process_exit(record_manager):
    node = record_manager
    node.is_recording = True
    process = node.rosbag_process = MagicMock()
    process.wait.side_effect = lambda: (
        node.rosbag_manager_mode_pub.publish.assert_not_called()
    )
    node.get_verified_metadata_client.call_async.return_value.result.return_value = None
    node.delete_last_recording = MagicMock()
    success, _ = node.stop_recording()
    assert not success  # Metadata failure still publishes the real stopped state.
    process.wait.assert_called_once()
    assert node.rosbag_manager_mode_pub.publish.call_args.args[0].data is False


def test_cancel_publishes_stopped_without_upload(record_manager):
    node = record_manager
    node.is_recording = True
    node.rosbag_process = MagicMock()
    node.recording_cancelled_pub = MagicMock()
    node.delete_last_recording = MagicMock()
    success, _ = node.cancel_recording()
    assert success
    assert node.rosbag_manager_mode_pub.publish.call_args.args[0].data is False
    node.recording_completed_pub.publish.assert_not_called()
    assert json.loads(node.recording_feedback_pub.publish.call_args.args[0].data)["state"] == "cancelled"


def test_cancellation_delete_failure_does_not_announce_cancelled(record_manager):
    node = record_manager
    node.is_recording = True
    node.rosbag_process = MagicMock()
    node.delete_last_recording = MagicMock(return_value=False)
    success, _ = node.cancel_recording()
    assert not success
    assert json.loads(node.recording_feedback_pub.publish.call_args.args[0].data)["state"] == "failed"
