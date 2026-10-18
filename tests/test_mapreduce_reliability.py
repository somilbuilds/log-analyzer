import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from api import hadoop
from api.mapreduce_status import combine_mapreduce_status, parse_yarn_application_status
from batch.mapreduce_job import publish_output
from batch.mapreduce_job.publish_output import publish_after_driver, publish_staged_output


class FakeHdfs:
    def __init__(self, paths=None):
        self.paths = dict(paths or {})
        self.failures = []

    def __call__(self, command):
        args = command[2:]
        if self.failures and self.failures[0](args):
            self.failures.pop(0)
            return subprocess.CompletedProcess(command, 9, "", "simulated command failure")
        op, rest = args[0], args[1:]
        if op == "-test" and rest[0] in ("-e", "-r"):
            path = rest[1]
            parent, _, name = path.rpartition("/")
            present = path in self.paths or (parent in self.paths and name in self.paths[parent])
            return subprocess.CompletedProcess(command, 0 if present else 1, "", "")
        if op == "-ls":
            path = rest[0]
            if path not in self.paths:
                return subprocess.CompletedProcess(command, 1, "", "path missing")
            children = self.paths[path]
            listing = "\n".join("-rw-r--r-- 1 user group {} 2026-01-01 00:00 {}/{}".format(
                len(data), path, name
            ) for name, data in children.items())
            return subprocess.CompletedProcess(command, 0, listing, "")
        if op == "-cat":
            path = rest[0]
            parent, name = path.rsplit("/", 1)
            if parent not in self.paths or name not in self.paths[parent]:
                return subprocess.CompletedProcess(command, 1, "", "part missing")
            return subprocess.CompletedProcess(command, 0, self.paths[parent][name], "")
        if op == "-mkdir":
            path = rest[0]
            if path in self.paths:
                return subprocess.CompletedProcess(command, 1, "", "File exists")
            self.paths[path] = {}
            return subprocess.CompletedProcess(command, 0, "", "")
        if op == "-mv":
            source, destination = rest
            if source not in self.paths or destination in self.paths:
                return subprocess.CompletedProcess(command, 1, "", "rename refused")
            self.paths[destination] = self.paths.pop(source)
            return subprocess.CompletedProcess(command, 0, "", "")
        if op == "-rm":
            self.paths.pop(rest[-1], None)
            return subprocess.CompletedProcess(command, 0, "", "")
        if op == "-rmdir":
            self.paths.pop(rest[0], None)
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.CompletedProcess(command, 2, "", "unsupported fake command")


class MapReducePublicationTests(unittest.TestCase):
    @patch("batch.mapreduce_job.publish_output.subprocess.run")
    def test_default_publisher_runner_uses_python35_compatible_capture_options(self, run):
        run.return_value = subprocess.CompletedProcess(["hdfs"], 0, "ok", "")
        result = publish_output._run_subprocess(["hdfs", "dfs", "-ls", "/output"])
        self.assertEqual(result.returncode, 0)
        kwargs = run.call_args.kwargs
        self.assertNotIn("capture_output", kwargs)
        self.assertNotIn("text", kwargs)
        self.assertEqual(kwargs["stdout"], subprocess.PIPE)
        self.assertEqual(kwargs["stderr"], subprocess.PIPE)
        self.assertTrue(kwargs["universal_newlines"])

    def test_command_logging_quotes_with_python35_compatible_shlex(self):
        self.assertEqual(
            publish_output._format_command(["hdfs", "dfs", "-ls", "/path with spaces"]),
            "hdfs dfs -ls '/path with spaces'",
        )

    def test_successful_publication_replaces_previous_result_only_after_validation(self):
        hdfs = FakeHdfs({
            "/results/hosts": {"part-00000": "old result"},
            "/results/hosts.stage-1": {"part-00000": "new result"},
        })
        self.assertEqual(publish_staged_output(
            "hosts", "/results/hosts", "/results/hosts.stage-1", "job-1", hdfs,
        ), 0)
        self.assertEqual(hdfs.paths["/results/hosts"]["part-00000"], "new result")
        self.assertFalse(any("backup" in path for path in hdfs.paths))

    def test_missing_staged_output_preserves_previous_result_and_cleans_stage(self):
        hdfs = FakeHdfs({
            "/results/hosts": {"part-00000": "old result"},
            "/results/hosts.stage-2": {"_SUCCESS": ""},
        })
        self.assertEqual(publish_staged_output(
            "hosts", "/results/hosts", "/results/hosts.stage-2", "job-2", hdfs,
        ), 1)
        self.assertEqual(hdfs.paths["/results/hosts"]["part-00000"], "old result")
        self.assertNotIn("/results/hosts.stage-2", hdfs.paths)

    def test_publication_failure_rolls_back_previous_result(self):
        hdfs = FakeHdfs({
            "/results/hosts": {"part-00000": "old result"},
            "/results/hosts.stage-3": {"part-00000": "new result"},
        })
        hdfs.failures.append(lambda args: args[:1] == ["-mv"] and args[1].endswith("stage-3"))
        self.assertEqual(publish_staged_output(
            "hosts", "/results/hosts", "/results/hosts.stage-3", "job-3", hdfs,
        ), 1)
        self.assertEqual(hdfs.paths["/results/hosts"]["part-00000"], "old result")
        self.assertNotIn("/results/hosts.stage-3", hdfs.paths)

    def test_post_publish_read_failure_restores_previous_result(self):
        hdfs = FakeHdfs({
            "/results/hosts": {"part-00000": "old result"},
            "/results/hosts.stage-4": {"part-00000": "new result"},
        })
        hdfs.failures.append(lambda args: args[:2] == ["-test", "-r"] and args[2].startswith("/results/hosts/"))
        self.assertEqual(publish_staged_output(
            "hosts", "/results/hosts", "/results/hosts.stage-4", "job-4", hdfs,
        ), 1)
        self.assertEqual(hdfs.paths["/results/hosts"]["part-00000"], "old result")
        self.assertTrue(any(".failed.job-4" in path for path in hdfs.paths))

    def test_failed_job_does_not_enter_publication_path(self):
        hdfs = FakeHdfs({"/results/hosts": {"part-00000": "old result"}})
        self.assertEqual(publish_after_driver(
            7, "hosts", "/results/hosts", "/results/hosts.stage-failed", "job-failed", hdfs,
        ), 7)
        self.assertEqual(hdfs.paths["/results/hosts"]["part-00000"], "old result")
        self.assertNotIn("/results/hosts.stage-failed", hdfs.paths)
        self.assertFalse(any(".__publish_lock" in path for path in hdfs.paths))


class YarnStateTests(unittest.TestCase):
    def test_yarn_states_and_publication_are_independent(self):
        parsed = parse_yarn_application_status(
            "State : FINISHED\nFinal-State : SUCCEEDED\nDiagnostics : none\n"
            "Tracking-URL : http://rm/app\nStart-Time : 100\nFinish-Time : 200\n"
        )
        self.assertEqual(parsed["status"], "succeeded")
        self.assertEqual(parsed["tracking_url"], "http://rm/app")
        self.assertEqual(combine_mapreduce_status(parsed, False, 0)["status"], "succeeded")
        self.assertEqual(combine_mapreduce_status(parsed, True, None)["status"], "publishing")
        running = parse_yarn_application_status("State : RUNNING\nFinal-State : UNDEFINED\n")
        running_state = combine_mapreduce_status(running, True, None)
        self.assertEqual(running_state["status"], "running")
        self.assertTrue(running_state["running"])
        failed = parse_yarn_application_status("State : FAILED\nFinal-State : FAILED\n")
        self.assertEqual(combine_mapreduce_status(failed, False, 0)["status"], "failed")
        killed = parse_yarn_application_status("State : KILLED\nFinal-State : KILLED\n")
        self.assertEqual(combine_mapreduce_status(killed, False, 1)["status"], "killed")
        incomplete = parse_yarn_application_status("State : FINISHED\nFinal-State : UNDEFINED\n")
        self.assertEqual(combine_mapreduce_status(incomplete, False, 0)["status"], "unknown")

    def test_client_exit_before_yarn_final_state_and_rm_failure_are_unknown(self):
        unknown = parse_yarn_application_status("State : RUNNING\n")
        self.assertEqual(combine_mapreduce_status(unknown, False, 0)["status"], "unknown")
        unavailable = {"available": False, "status": "unknown", "application_id": "application_1_1"}
        unavailable_state = combine_mapreduce_status(unavailable, True, None)
        self.assertEqual(unavailable_state["status"], "unknown")
        self.assertTrue(unavailable_state["running"])


class HdfsDatasetValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.NamedTemporaryFile(delete=False)
        self.temp.close()
        self.exists = patch("api.hadoop.os.path.exists", return_value=True)
        self.exists.start()
        self.raw_file = patch("api.hadoop._raw_file", return_value=self.temp.name)
        self.raw_file.start()

    def tearDown(self):
        self.exists.stop()
        self.raw_file.stop()
        os.unlink(self.temp.name)

    @patch("api.hadoop._run_docker_exec")
    def test_existing_input_is_not_uploaded(self, docker_exec):
        docker_exec.return_value = {"status": "success", "output": "", "returncode": 0}
        result = hadoop.ensure_hdfs_dataset("j1", "hosts")
        self.assertTrue(result["available"])
        self.assertTrue(result["exists"])
        docker_exec.assert_called_once()

    @patch("api.hadoop.subprocess.run")
    @patch("api.hadoop._run_docker_exec")
    def test_confirmed_missing_input_uploads_without_force_then_verifies(self, docker_exec, run):
        docker_exec.side_effect = [
            {"status": "error", "returncode": 1, "stdout": "", "stderr": "", "error_kind": "command_failed"},
            {"status": "success", "output": "", "returncode": 0},
        ]
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        result = hadoop.ensure_hdfs_dataset("j2", "hosts")
        self.assertTrue(result["available"])
        command = docker_exec.call_args_list[1].args[1][2]
        self.assertIn("hdfs dfs -put /tmp/access_log", command)
        self.assertNotIn("-put -f", command)

    @patch("api.hadoop.subprocess.run")
    @patch("api.hadoop._run_docker_exec")
    def test_uncertain_validation_never_uploads(self, docker_exec, run):
        for check in (
            {"status": "error", "returncode": None, "stdout": "", "stderr": "timeout", "error_kind": "timeout"},
            {"status": "error", "returncode": 1, "stdout": "", "stderr": "Connection refused", "error_kind": "command_failed"},
        ):
            docker_exec.reset_mock()
            docker_exec.return_value = check
            result = hadoop.ensure_hdfs_dataset("j3", "hosts")
            self.assertFalse(result["available"])
            run.assert_not_called()
            docker_exec.assert_called_once()

    @patch("api.hadoop.subprocess.run")
    @patch("api.hadoop._run_docker_exec")
    def test_docker_or_upload_failures_are_reported(self, docker_exec, run):
        docker_exec.return_value = {"status": "error", "returncode": 1, "stdout": "", "stderr": "", "error_kind": "command_failed"}
        run.return_value = subprocess.CompletedProcess([], 17, "", "docker cp failed")
        result = hadoop.ensure_hdfs_dataset("j4", "hosts")
        self.assertFalse(result["available"])
        self.assertEqual(result["error_kind"], "docker_copy_failed")
        docker_exec.reset_mock()
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        docker_exec.side_effect = [
            {"status": "error", "returncode": 1, "stdout": "", "stderr": "", "error_kind": "command_failed"},
            {"status": "error", "returncode": 9, "stdout": "", "stderr": "put failed", "output": "put failed", "error_kind": "command_failed"},
        ]
        result = hadoop.ensure_hdfs_dataset("j5", "hosts")
        self.assertFalse(result["available"])
        self.assertEqual(result["error_kind"], "command_failed")

    @patch("api.hadoop.subprocess.run")
    def test_docker_exec_preserves_error_code_stderr_and_timeout(self, run):
        run.return_value = subprocess.CompletedProcess([], 12, b"", b"NameNode refused connection")
        failed = hadoop._run_docker_exec("namenode", ["hdfs", "dfs", "-test", "-e", "/x"])
        self.assertEqual(failed["returncode"], 12)
        self.assertIn("refused", failed["stderr"])
        run.side_effect = subprocess.TimeoutExpired(["docker"], 5, output=b"partial", stderr=b"waiting")
        timed_out = hadoop._run_docker_exec("namenode", ["yarn", "application", "-status", "application_1_1"])
        self.assertEqual(timed_out["error_kind"], "timeout")
        self.assertIn("partial", timed_out["stdout"])


if __name__ == "__main__":
    unittest.main()
