import logging
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from api.mapreduce_logging import (
    attach_job_log_handler, configure_logging, monitor_driver_process,
    parse_driver_line, should_log_http_request,
)


class MapReduceLoggingTests(unittest.TestCase):
    def setUp(self):
        self.root = logging.getLogger()
        self.saved_root_handlers = self.root.handlers[:]
        self.saved_root_level = self.root.level
        self.mr_logger = logging.getLogger("clarknet.mapreduce")
        self.saved_mr_handlers = self.mr_logger.handlers[:]
        self.saved_mr_level = self.mr_logger.level
        self.saved_mr_propagate = self.mr_logger.propagate

    def temporary_workspace_dir(self):
        parent = os.path.join(os.path.dirname(__file__), ".test-tmp")
        os.makedirs(parent, exist_ok=True)
        return tempfile.mkdtemp(dir=parent)

    def remove_workspace_dir(self, path):
        for handler in self.mr_logger.handlers:
            if handler not in self.saved_mr_handlers:
                handler.close()
        self.mr_logger.handlers.clear()
        self.mr_logger.handlers.extend(self.saved_mr_handlers)
        shutil.rmtree(path)

    def tearDown(self):
        self.root.handlers.clear()
        self.root.handlers.extend(self.saved_root_handlers)
        self.root.setLevel(self.saved_root_level)
        self.mr_logger.handlers.clear()
        self.mr_logger.handlers.extend(self.saved_mr_handlers)
        self.mr_logger.setLevel(self.saved_mr_level)
        self.mr_logger.propagate = self.saved_mr_propagate

    def test_successful_access_requests_are_suppressed_but_failures_are_kept(self):
        self.assertFalse(should_log_http_request(200))
        self.assertFalse(should_log_http_request(302))
        self.assertTrue(should_log_http_request(404))
        self.assertTrue(should_log_http_request(500))

    def test_hadoop_output_parser_extracts_yarn_ids_progress_and_errors(self):
        accepted = parse_driver_line("Submitted application application_1700000000000_0042")
        self.assertEqual(accepted["application_id"], "application_1700000000000_0042")
        self.assertEqual(accepted["level"], logging.INFO)
        progress = parse_driver_line("map 40% reduce 10%")
        self.assertEqual(progress["progress"], {"map": 40, "reduce": 10})
        failure = parse_driver_line("ERROR BlockMissingException: block blk_123 is missing")
        self.assertEqual(failure["level"], logging.ERROR)
        self.assertEqual(failure["event"], "hadoop.error")

    def test_terminal_is_concise_while_debug_file_keeps_full_records(self):
        log_dir = self.temporary_workspace_dir()
        try:
            terminal = __import__("io").StringIO()
            with patch("api.mapreduce_logging.sys.stdout", terminal):
                configure_logging(log_dir, "DEBUG")
                context = {"component": "hadoop-driver", "job_id": "job-test-17", "task_id": "hosts"}
                self.mr_logger.debug("event=driver.output detail=%r", "verbose diagnostic", extra=context)
                self.mr_logger.info("event=mapreduce.progress map_pct=50", extra=context)
                for handler in self.mr_logger.handlers:
                    handler.flush()
            terminal_text = terminal.getvalue()
            with open(os.path.join(log_dir, "mapreduce.log"), encoding="utf-8") as stream:
                summary_text = stream.read()
            with open(os.path.join(log_dir, "mapreduce-debug.log"), encoding="utf-8") as stream:
                debug_text = stream.read()
            self.assertEqual(terminal_text, "")  # MapReduce is displayed via the concise summary log tail.
            self.assertIn("map_pct=50", summary_text)
            self.assertNotIn("verbose diagnostic", summary_text)
            self.assertIn("verbose diagnostic", debug_text)
            self.assertIn("job_id=job-test-17 task_id=hosts", debug_text)
        finally:
            self.remove_workspace_dir(log_dir)

    def test_driver_failure_preserves_exit_code_error_and_job_correlation(self):
        log_dir = self.temporary_workspace_dir()
        try:
            configure_logging(log_dir, "DEBUG")
            process = subprocess.Popen(
                [sys.executable, "-c", (
                    "print('2026-10-18T00:00:00Z INFO component=mapreduce-shell "
                    "job_id=job-test-18 task_id=hosts stage=input_validation "
                    "path=/user/data/raw_logs/access_log result=present'); "
                    "print('Submitted application application_1700000000000_0042'); "
                    "import sys; print('ERROR BlockMissingException: missing blk_123', file=sys.stderr); "
                    "print('Map input records=42'); "
                    "raise SystemExit(7)"
                )],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            result = monitor_driver_process(process, "job-test-18", "hosts", self.mr_logger)
            for handler in self.mr_logger.handlers:
                handler.flush()
            self.assertEqual(result["return_code"], 7)
            self.assertEqual(result["application_id"], "application_1700000000000_0042")
            with open(os.path.join(log_dir, "mapreduce.log"), encoding="utf-8") as stream:
                summary_text = stream.read()
            with open(os.path.join(log_dir, "mapreduce-debug.log"), encoding="utf-8") as stream:
                debug_text = stream.read()
            self.assertIn("ERROR", summary_text)
            self.assertIn("BlockMissingException", summary_text)
            self.assertIn("exit_code=7", summary_text)
            self.assertIn("application_1700000000000_0042", summary_text)
            self.assertIn("component=mapreduce-shell", summary_text)
            self.assertIn("event=mapreduce.input_validation", summary_text)
            self.assertIn("job_id=job-test-18 task_id=hosts", debug_text)
            self.assertIn("Map input records=42", debug_text)
        finally:
            self.remove_workspace_dir(log_dir)

    def test_streaming_worker_logs_mapper_exception_with_job_id(self):
        runner = os.path.join(os.path.dirname(__file__), "..", "batch", "mapreduce_job", "streaming_runner.py")
        temp_dir = self.temporary_workspace_dir()
        try:
            mapper = os.path.join(temp_dir, "broken_mapper.py")
            with open(mapper, "w", encoding="utf-8") as script:
                script.write("raise ValueError('mapper exploded')\n")
            env = os.environ.copy()
            env.update({"BDA_JOB_ID": "job-test-19", "BDA_TASK_ID": "hosts"})
            result = subprocess.run(
                [sys.executable, runner, "mapper", mapper], input="one record\n",
                capture_output=True, text=True, env=env, check=False,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("event=worker.failed", result.stderr)
            self.assertIn("job_id=job-test-19 task_id=hosts", result.stderr)
            self.assertIn("ValueError: mapper exploded", result.stderr)
        finally:
            self.remove_workspace_dir(temp_dir)

    def test_per_job_log_handlers_append_instead_of_truncating(self):
        temp_dir = self.temporary_workspace_dir()
        path = os.path.join(temp_dir, "mapreduce-job-123.log")
        logger = logging.getLogger("test.mapreduce.job.append")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        handlers = []
        try:
            for event in ("first-run", "second-run"):
                handler = attach_job_log_handler(logger, path, "job-123", "hosts")
                handlers.append(handler)
                logger.info("event=%s", event, extra={"component": "mapreduce-api"})
                logger.removeHandler(handler)
                handler.close()
            with open(path, encoding="utf-8") as stream:
                content = stream.read()
            self.assertIn("first-run", content)
            self.assertIn("second-run", content)
            self.assertEqual(content.count("job_id=job-123 task_id=hosts"), 2)
        finally:
            for handler in handlers:
                if handler in logger.handlers:
                    logger.removeHandler(handler)
                    handler.close()
            shutil.rmtree(temp_dir)


    def test_monitor_driver_process_reports_map_and_reduce_progress_and_phases(self):
        log_dir = self.temporary_workspace_dir()
        progress_updates = []
        phase_updates = []
        try:
            configure_logging(log_dir, "DEBUG")
            process = subprocess.Popen(
                [sys.executable, "-c", (
                    "print('[1/4] Ensuring raw ClarkNet log exists in HDFS...'); "
                    "print('Submitted application application_1700000000000_0099'); "
                    "print('map 20% reduce 0%'); "
                    "print('map 100% reduce 0%'); "
                    "print('map 100% reduce 50%'); "
                    "print('map 100% reduce 100%'); "
                    "print('[4/4] Validating and publishing output: /user/data/results/status'); "
                    "raise SystemExit(0)"
                )],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            result = monitor_driver_process(
                process, "job-progress-1", "status", self.mr_logger,
                on_progress=lambda m, r: progress_updates.append((m, r)),
                on_phase=lambda p: phase_updates.append(p),
            )
            self.assertEqual(result["return_code"], 0)
            self.assertEqual(result["application_id"], "application_1700000000000_0099")
            self.assertIn((20, 0), progress_updates)
            self.assertIn((100, 0), progress_updates)
            self.assertIn((100, 50), progress_updates)
            self.assertIn((100, 100), progress_updates)
            self.assertIn("validating_input", phase_updates)
            self.assertIn("mapping", phase_updates)
            self.assertIn("reducing", phase_updates)
            self.assertIn("publishing", phase_updates)
        finally:
            self.remove_workspace_dir(log_dir)


if __name__ == "__main__":
    unittest.main()
