#!/usr/bin/env python3
import json
import os
import shutil
import signal
import argparse
import subprocess
from datetime import datetime, timezone
import threading
import time
from typing import Optional, Tuple

import rclpy
from rclpy.node import Node
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy

from rosgraph_msgs.msg import Clock
from std_msgs.msg import Bool, Int64, String
from std_srvs.srv import Trigger
from airoa_data_msgs.srv import StringTrigger

from yubi_core.backend_client import create_backend
from yubi_core.recording_process import start_recorder, stop_failed_recorder

STResponse = StringTrigger.Response
STRequest = StringTrigger.Request


class RecordManager(Node):
    def __init__(self):
        super().__init__("record_manager")
        self.rosbag_process: Optional[subprocess.Popen] = None
        self.is_recording = False
        self._recording_lock = threading.RLock()
        self.recent_record_dir: Optional[str] = None
        self.task_name: Optional[str] = None
        self.reent_cg = ReentrantCallbackGroup()

        self.__setup_parameters()
        self.__setup_publishers()
        self.__setup_clients()
        self.__setup_services()

    def __setup_parameters(self):
        # fixed parameters
        self.declare_parameter("base_url", "http://localhost:8000/api")
        self.declare_parameter("api_key", "")
        self.declare_parameter("offline_mode", False)
        self.declare_parameter("task_file", "")
        self.declare_parameter("site", "")
        self.declare_parameter("location", "")
        self.declare_parameter("record_topics", ["/tf"])
        self.declare_parameter("record_base_dir", "/root/datasets/rosbags")
        self.declare_parameter("required_free_space", 100)  # in GB
        self.declare_parameter("rosbag_params", ["--storage", "mcap"])
        self.declare_parameter("qos_overrides_file", "")

        profile = self._fetch_robot_profile()
        self.robot_id = profile["id"]
        self.site = str(self.get_parameter("site").value)
        self.location = str(self.get_parameter("location").value)
        self.record_topics = list(self.get_parameter("record_topics").value)
        self.record_base_dir = str(self.get_parameter("record_base_dir").value)
        required_space_gb = int(self.get_parameter("required_free_space").value)
        self.required_free_space = required_space_gb * 1024 * 1024 * 1024  # Convert to bytes
        self.rosbag_params = list(self.get_parameter("rosbag_params").value)
        # use_sim_time is a built-in ROS 2 parameter (declared by Node base class)
        self.use_sim_time = self.get_parameter("use_sim_time").value
        self._sim_time_verified = False  # set True after /clock confirmed
        self.qos_overrides_file = str(self.get_parameter("qos_overrides_file").value)
        if self.qos_overrides_file:
            # A comments-only/empty overrides file parses to None and makes
            # `ros2 bag record --qos-profile-overrides-path` crash (seen on
            # jazzy: NoneType has no .items()). Only pass the flag when the
            # file actually contains entries; otherwise fall back to default QoS.
            overrides = None
            try:
                import yaml

                with open(self.qos_overrides_file) as f:
                    overrides = yaml.safe_load(f)
            except Exception as exc:
                self.get_logger().warn(
                    f"Could not read QoS overrides file {self.qos_overrides_file}: {exc}"
                )
            if isinstance(overrides, dict) and overrides:
                self.get_logger().info(f"QoS overrides file: {self.qos_overrides_file}")
            else:
                self.get_logger().info(
                    "QoS overrides file has no entries — using default QoS"
                )
                self.qos_overrides_file = ""

        # v2.0 metadata parameters — auto-fetched from backend when set to "FIXME"
        self.declare_parameter("robot_type", "FIXME")
        self.declare_parameter("environment_type", "real_world")
        self.declare_parameter("runner_organization", "FIXME")
        self.declare_parameter("devices", "[]")  # JSON string param

        # Resolve FIXME placeholders: backend → config → "unknown"
        robot_type = str(self.get_parameter("robot_type").value)
        if robot_type == "FIXME":
            resolved = profile.get("model") or "unknown"
            self.set_parameters([rclpy.Parameter("robot_type", value=resolved)])
            self.get_logger().info(f"Resolved robot_type: {resolved}")

        runner_org = str(self.get_parameter("runner_organization").value)
        if runner_org == "FIXME":
            resolved = profile.get("organization_name") or "unknown"
            self.set_parameters([rclpy.Parameter("runner_organization", value=resolved)])
            self.get_logger().info(f"Resolved runner_organization: {resolved}")

        # Override location from backend if not set in config
        if not self.location and profile.get("location_name"):
            self.location = profile["location_name"]
            self.get_logger().info(f"Resolved location from backend: {self.location}")

        # dynamic parameters for teleop interface metadata
        self.declare_parameter("teleop_interface", "unknown")
        self.declare_parameter("teleop_interface_url", "unknown")
        self.declare_parameter("teleop_interface_hash", "unknown")
        self.declare_parameter("teleop_interface_branch", "unknown")

        os.makedirs(self.record_base_dir, exist_ok=True)

        self.record_url = "https://github.com/airoa-org/yubi_core"
        self.record_hash = os.getenv("GIT_HASH", "unknown")
        self.record_branch = os.getenv("GIT_BRANCH", "unknown")

    def _fetch_robot_profile(self) -> dict:
        """Fetch robot profile from /robot/me.

        Returns dict with ``id``, ``model``, and ``organization_id`` keys.
        Missing or unreachable values fall back to defaults.
        """
        base_url = str(self.get_parameter("base_url").value)
        api_key = str(self.get_parameter("api_key").value)
        offline_mode = bool(self.get_parameter("offline_mode").value)
        task_file = str(self.get_parameter("task_file").value)
        defaults = {"id": "unknown", "model": None, "organization_name": None, "location_name": None}
        if not api_key and not offline_mode:
            self.get_logger().warn("No api_key configured and offline_mode is off")
            return defaults
        try:
            client = create_backend(
                offline_mode=offline_mode, task_file=task_file,
                base_url=base_url, api_key=api_key,
            )
            data = client.get_robot_self()
            if data:
                profile = {
                    "id": str(data.get("id", "unknown")),
                    "model": data.get("model"),
                    "organization_name": data.get("organization_name"),
                    "location_name": data.get("location_name"),
                }
                self.get_logger().info(f"Fetched robot profile from API: {profile}")
                return profile
            self.get_logger().warn("API returned no robot data")
            return defaults
        except Exception as e:
            self.get_logger().warn(f"Failed to fetch robot profile: {e}")
            return defaults

    def _check_clock_available(self, timeout_sec: float = 2.0) -> bool:
        """Check if /clock topic is actively publishing messages."""
        event = threading.Event()

        def _on_clock(msg):
            event.set()

        sub = self.create_subscription(Clock, "/clock", _on_clock, 10)
        try:
            ok = event.wait(timeout=timeout_sec)
            if ok:
                self.get_logger().info("/clock is active — enabling --use-sim-time")
            else:
                self.get_logger().warn(
                    f"/clock not received within {timeout_sec}s"
                )
            return ok
        finally:
            self.destroy_subscription(sub)

    def __setup_publishers(self):
        latched_qos = QoSProfile(depth=1)
        latched_qos.reliability = ReliabilityPolicy.RELIABLE
        latched_qos.durability = DurabilityPolicy.TRANSIENT_LOCAL

        self.rosbag_manager_mode_pub = self.create_publisher(Bool, "~/recording", latched_qos)
        # One ordered stream carries confirmed lifecycle outcomes. A Bool cannot
        # distinguish a saved episode from a cancellation or recorder failure.
        self.recording_feedback_pub = self.create_publisher(
            String, "~/recording_feedback", latched_qos
        )
        self._publish_recording_feedback("idle")
        self.storage_free_space_pub = self.create_publisher(Int64, "~/free", 10)
        self.storage_used_space_pub = self.create_publisher(Int64, "~/used", 10)

        completed_qos = QoSProfile(depth=10)
        completed_qos.reliability = ReliabilityPolicy.RELIABLE
        completed_qos.durability = DurabilityPolicy.TRANSIENT_LOCAL
        self.recording_completed_pub = self.create_publisher(
            String, "~/recording_completed", completed_qos
        )
        self.recording_cancelled_pub = self.create_publisher(
            String, "~/recording_cancelled", completed_qos
        )

        self.create_timer(1.0, self.publish_storage_statistics)
        self.create_timer(1.0, self.publish_recording_state)

    def __setup_clients(self):
        self.init_meta_client = self.create_client(
            Trigger, "/metadata_handler/initialize_metadata", callback_group=self.reent_cg
        )
        self.add_file_client = self.create_client(
            StringTrigger, "/metadata_handler/add_file", callback_group=self.reent_cg
        )
        self.set_robot_client = self.create_client(
            StringTrigger, "/metadata_handler/set_robot", callback_group=self.reent_cg
        )
        self.set_environment_client = self.create_client(
            StringTrigger, "/metadata_handler/set_environment", callback_group=self.reent_cg
        )
        self.extend_programs_client = self.create_client(
            StringTrigger, "/metadata_handler/extend_programs", callback_group=self.reent_cg
        )
        self.set_devices_client = self.create_client(
            StringTrigger, "/metadata_handler/set_devices", callback_group=self.reent_cg
        )
        self.get_verified_metadata_client = self.create_client(
            Trigger, "/metadata_handler/get_verified_metadata", callback_group=self.reent_cg
        )

        for c in [
            self.init_meta_client,
            self.add_file_client,
            self.set_robot_client,
            self.set_environment_client,
            self.extend_programs_client,
            self.set_devices_client,
            self.get_verified_metadata_client,
        ]:
            while not c.wait_for_service(timeout_sec=1.0):
                self.get_logger().info(f"Waiting for service {c.srv_name} to become available...")

    def __setup_services(self):
        self.create_service(StringTrigger, "~/start_recording", self.start_recording_srv, callback_group=self.reent_cg)
        self.create_service(Trigger, "~/stop_recording", self.stop_recording_srv, callback_group=self.reent_cg)
        self.create_service(Trigger, "~/cancel_recording", self.cancel_recording_srv, callback_group=self.reent_cg)
        self.create_service(Trigger, "~/delete_recording", self.delete_recording_srv)
        self.create_service(
            StringTrigger, "~/toggle_recording", self.toggle_recording_srv, callback_group=self.reent_cg
        )

    def publish_storage_statistics(self):
        total, used, free = shutil.disk_usage(self.record_base_dir)

        free_msg = Int64()
        free_msg.data = int(free)
        self.storage_free_space_pub.publish(free_msg)

        used_msg = Int64()
        used_msg.data = int(used)
        self.storage_used_space_pub.publish(used_msg)

    def publish_recording_state(self):
        # A timer must not occupy an executor thread waiting for a service
        # callback that itself needs executor threads for metadata responses.
        if not self._recording_lock.acquire(blocking=False):
            return
        try:
            if self.is_recording and self.rosbag_process is not None:
                result = self.rosbag_process.poll()
                if result is not None:
                    self.is_recording = False
                    self.rosbag_process = None
                    self.get_logger().error(
                        f"Rosbag recorder exited unexpectedly (status {result}); "
                        "recording stopped, data has not been verified or uploaded."
                    )
                    self._publish_recording_feedback("failed")
            msg = Bool()
            msg.data = self.is_recording
            self.rosbag_manager_mode_pub.publish(msg)
        finally:
            self._recording_lock.release()

    def _publish_recording_feedback(self, state):
        message = String()
        message.data = json.dumps({
            "state": state,
            "recording_id": self.task_name or "",
        })
        self.recording_feedback_pub.publish(message)

    def wait_for_future_done(self, future: rclpy.Future, timeout: float = 1.0, sleep_time: float = 0.01):
        elapsed = 0.0
        while not future.done() and rclpy.ok() and elapsed < timeout:
            time.sleep(sleep_time)
            elapsed += sleep_time

    def start_recording(self, message: Optional[str]) -> Tuple[bool, str]:
        with self._recording_lock:
            return self._start_recording(message)

    def _start_recording(self, message: Optional[str]) -> Tuple[bool, str]:
        task_name = message or datetime.now(timezone.utc).strftime("rosbag-%Y%m%d-%H%M%S")
        total, used, free = shutil.disk_usage(self.record_base_dir)
        self.get_logger().info(f"Free space: {free}, Required: {self.required_free_space}")

        if free < self.required_free_space:
            self.get_logger().warn("Not enough space to record rosbag.")
            return False, "Not enough space to record rosbag"

        if self.is_recording:
            self.get_logger().warn("Rosbag is already recording.")
            return False, "Rosbag is already recording."

        record_dir = os.path.join(self.record_base_dir, task_name)
        os.makedirs(self.record_base_dir, exist_ok=True)
        
        # check save format is mcap or not
        parser = argparse.ArgumentParser()
        parser.add_argument('--storage', type=str)
        args, unknown = parser.parse_known_args(self.rosbag_params)
        if vars(args).get('storage', "") == 'mcap':
            bag_filename = task_name + "_0.mcap"
        else:
            # TODO: support other storage formats if needed
            raise ValueError("Only 'mcap' storage format is supported.")


        # initialize metadata
        init_metadata_req = Trigger.Request()
        init_metadata_future = self.init_meta_client.call_async(init_metadata_req)
        self.wait_for_future_done(init_metadata_future)

        if init_metadata_future.result() is None or not init_metadata_future.result().success:
            self.get_logger().error("Failed to initialize metadata.")
            return False, "Failed to initialize metadata."
        self.get_logger().info("Metadata initialized.")

        # add file entry
        add_file_req = StringTrigger.Request()
        add_file_req.message = json.dumps(
            {
                "type": "mcap",
                "name": bag_filename,
            }
        )
        add_file_future = self.add_file_client.call_async(add_file_req)
        self.wait_for_future_done(add_file_future)

        if add_file_future.result() is None or not add_file_future.result().success:
            self.get_logger().error("Failed to add file entry to metadata.")
            return False, "Failed to add file entry to metadata."

        self.get_logger().info("File entry added to metadata.")

        # set robot
        set_robot_req = StringTrigger.Request()
        robot_type = str(self.get_parameter("robot_type").value)
        set_robot_req.message = json.dumps({"type": robot_type, "id": self.robot_id})
        set_robot_future = self.set_robot_client.call_async(set_robot_req)
        self.wait_for_future_done(set_robot_future)

        if set_robot_future.result() is None or not set_robot_future.result().success:
            self.get_logger().error("Failed to set robot in metadata.")
            return False, "Failed to set robot in metadata."

        self.get_logger().info("Robot set in metadata.")

        # set environment
        set_environment_req = StringTrigger.Request()
        environment_type = str(self.get_parameter("environment_type").value)
        env = {"type": environment_type, "site": self.site}
        if self.location:
            env["location"] = self.location
        set_environment_req.message = json.dumps(env)
        set_environment_future = self.set_environment_client.call_async(set_environment_req)
        self.wait_for_future_done(set_environment_future)

        if set_environment_future.result() is None or not set_environment_future.result().success:
            self.get_logger().error("Failed to set environment in metadata.")
            return False, "Failed to set environment in metadata."

        self.get_logger().info("Environment set in metadata.")

        # extend programs
        extend_programs_req = StringTrigger.Request()
        extend_programs_req.message = json.dumps(
            [
                {
                    "role": "interface",
                    "name": self.get_parameter("teleop_interface").value,
                    "source": {
                        "git": {
                            "uri": self.get_parameter("teleop_interface_url").value,
                            "hash": self.get_parameter("teleop_interface_hash").value,
                            "branch": self.get_parameter("teleop_interface_branch").value,
                        }
                    },
                },
                {
                    "role": "data_collection",
                    "name": "record_manager",
                    "source": {
                        "git": {
                            "uri": self.record_url,
                            "hash": self.record_hash,
                            "branch": self.record_branch,
                        }
                    },
                },
            ]
        )
        extend_programs_future = self.extend_programs_client.call_async(extend_programs_req)
        self.wait_for_future_done(extend_programs_future)

        if extend_programs_future.result() is None or not extend_programs_future.result().success:
            self.get_logger().error("Failed to extend programs in metadata.")
            return False, "Failed to extend programs in metadata."

        self.get_logger().info("Programs extended in metadata.")

        # set devices
        set_devices_req = StringTrigger.Request()
        devices_json = str(self.get_parameter("devices").value)
        set_devices_req.message = devices_json
        set_devices_future = self.set_devices_client.call_async(set_devices_req)
        self.wait_for_future_done(set_devices_future)

        if set_devices_future.result() is None or not set_devices_future.result().success:
            self.get_logger().error("Failed to set devices in metadata.")
            return False, "Failed to set devices in metadata."

        self.get_logger().info("Devices set in metadata.")

        # Start rosbag2 recording
        command = ["ros2", "bag", "record", "-o", record_dir]
        if self.use_sim_time:
            if not self._sim_time_verified:
                self._sim_time_verified = self._check_clock_available()
            if self._sim_time_verified:
                command.append("--use-sim-time")
            else:
                self.get_logger().warn(
                    "use_sim_time requested but /clock not available — using wall clock"
                )
        command.extend(self.rosbag_params)
        # Keep the readiness marker and recorder identity deterministic. Pausing
        # is not part of this button-driven recording workflow.
        command.extend([
            "--node-name", "rosbag2_recorder", "--log-level", "info",
            "--disable-keyboard-controls",
        ])
        if self.qos_overrides_file:
            command.extend(["--qos-profile-overrides-path", self.qos_overrides_file])
        command.extend(self.record_topics)

        self.get_logger().info(f"Recording command: {' '.join(command)}")

        try:
            self.rosbag_process, ready = start_recorder(command)
        except OSError as exc:
            self.get_logger().error(f"Failed to start rosbag2: {exc}")
            return False, f"Failed to launch rosbag2: {exc}"

        if not ready:
            stop_failed_recorder(self.rosbag_process)
            self.rosbag_process = None
            self.get_logger().error(
                "Rosbag did not enter recording within 10 seconds; "
                f"startup aborted. Check recorder output and {record_dir}."
            )
            self.publish_recording_state()
            return False, "Rosbag failed to enter recording"

        # Successfully started recording
        self.is_recording = True
        self.task_name = task_name
        
        self.recent_record_dir = record_dir

        self.publish_recording_state()
        if not self.is_recording:
            return False, "Rosbag exited immediately after startup"
        self._publish_recording_feedback("recording")
        self.get_logger().info("Rosbag recording started.")
        return True, "Success"

    def stop_recording(self) -> Tuple[bool, str]:
        with self._recording_lock:
            return self._stop_recording()

    def _stop_recording(self) -> Tuple[bool, str]:
        if not self.is_recording or self.rosbag_process is None:
            self.get_logger().warn("Rosbag is not recording.")
            return False, "Rosbag is not recording."

        # check if metadata is valid
        get_verified_metadata_req = Trigger.Request()
        get_verified_metadata_future = self.get_verified_metadata_client.call_async(get_verified_metadata_req)
        self.wait_for_future_done(get_verified_metadata_future, timeout=5.0)

        # now we can stop the rosbag recording
        self.rosbag_process.send_signal(signal.SIGINT)
        self.rosbag_process.wait()
        self.rosbag_process = None
        self.is_recording = False
        self.publish_recording_state()

        if get_verified_metadata_future.result() is None or not get_verified_metadata_future.result().success:
            self.get_logger().error("Metadata verification failed. Remove the recording.")
            self.delete_last_recording()
            self._publish_recording_feedback("failed")
            return False, "Metadata verification failed. Recording removed."

        try:
            meta_dict = json.loads(get_verified_metadata_future.result().message)
            self.save_meta_file(meta_dict)
        except (ValueError, OSError) as exc:
            self.get_logger().error(f"Failed to save recording metadata: {exc}")
            self._publish_recording_feedback("failed")
            return False, f"Failed to save recording metadata: {exc}"

        # Notify uploader of completed recording
        completed_msg = String()
        completed_msg.data = self.recent_record_dir
        self.recording_completed_pub.publish(completed_msg)
        self._publish_recording_feedback("completed")

        self.publish_recording_state()
        self.get_logger().info("Rosbag recording stopped.")
        return True, "Success"

    def toggle_recording(self, message: Optional[str]) -> Tuple[bool, str]:
        if not self.is_recording:
            return self.start_recording(message)
        return self.stop_recording()

    def delete_last_recording(self) -> bool:
        if not self.recent_record_dir:
            self.get_logger().warn("No recent recording to remove.")
            return False

        target = self.recent_record_dir
        try:
            # make empty dir by removing and creating same dir
            shutil.rmtree(target)
            os.makedirs(target, exist_ok=True)
        except OSError as exc:
            self.get_logger().error(f"Failed to remove recording: {exc}")
            return False

        self.get_logger().info(f"Recent recording {target} has been removed.")
        self.recent_record_dir = None
        return True

    def start_recording_srv(self, request: STRequest, response: STResponse):
        success, message = self.start_recording(request.message)
        response.success = success
        response.message = message
        return response

    def stop_recording_srv(self, request: Trigger.Request, response: Trigger.Response):
        success, message = self.stop_recording()
        response.success = success
        response.message = message
        return response

    def cancel_recording(self) -> Tuple[bool, str]:
        with self._recording_lock:
            return self._cancel_recording()

    def _cancel_recording(self) -> Tuple[bool, str]:
        """Stop recording without uploading, notify cancellation, and delete."""
        if not self.is_recording or self.rosbag_process is None:
            self.get_logger().warn("Rosbag is not recording.")
            return False, "Rosbag is not recording."

        self.rosbag_process.send_signal(signal.SIGINT)
        self.rosbag_process.wait()
        self.rosbag_process = None
        self.is_recording = False
        self.publish_recording_state()

        # Notify storage node of cancellation (no upload)
        if self.recent_record_dir:
            cancelled_msg = String()
            cancelled_msg.data = self.recent_record_dir
            self.recording_cancelled_pub.publish(cancelled_msg)

        if not self.delete_last_recording():
            self._publish_recording_feedback("failed")
            return False, "Recording stopped, but cancellation could not delete data"
        self._publish_recording_feedback("cancelled")
        self.publish_recording_state()
        self.get_logger().info("Recording cancelled and deleted.")
        return True, "Recording cancelled"

    def cancel_recording_srv(self, request: Trigger.Request, response: Trigger.Response):
        success, message = self.cancel_recording()
        response.success = success
        response.message = message
        return response

    def toggle_recording_srv(self, request: STRequest, response: STResponse):
        success, message = self.toggle_recording(request.message)
        response.success = success
        response.message = message
        return response

    def delete_recording_srv(self, request: STRequest, response: STResponse):
        if self.is_recording:
            success, message = self.stop_recording()
            if not success:
                response.success = False
                response.message = message
                return response
        response.success = self.delete_last_recording()
        response.message = "Recent recording deleted." if response.success else "Nothing to delete."
        return response

    def save_meta_file(self, meta_data: dict):

        meta_data_file = os.path.join(self.record_base_dir, self.task_name, "meta.json")
        with open(meta_data_file, "w", encoding="utf-8") as handle:
            json.dump(meta_data, handle, ensure_ascii=False, indent=2)

    def shutdown(self):
        if self.is_recording:
            self.get_logger().info("Stopping recording before shutdown.")
            self.stop_recording()
        self.destroy_node()


def main():
    from yubi_core.sentry_setup import init_sentry
    init_sentry()
    rclpy.init()
    record_manager = RecordManager()

    executor = MultiThreadedExecutor()

    try:
        rclpy.spin(record_manager, executor)
    except KeyboardInterrupt:
        print("Keyboard interrupt received, shutting down record manager.")
    finally:
        executor.shutdown()
        record_manager.shutdown()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
