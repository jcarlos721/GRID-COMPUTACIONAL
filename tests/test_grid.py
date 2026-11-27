# Testes de integração TCP entre Master e múltiplos Workers em modelo Pull com Heartbeat.

from __future__ import annotations

import socket
import threading
import unittest

from src.master import MasterNode
from src.protocol import LineBuffer, MessageType, encode_message
from src.sequential_baseline import run_sequential_baseline
from src.task_manager import TaskState
from src.worker import WorkerNode


class TestGridIntegration(unittest.TestCase):
    def test_master_with_multiple_workers_matches_baseline(self) -> None:
        n = 120_000
        chunk_size = 7_500
        baseline = run_sequential_baseline(n)

        master = MasterNode(
            host="127.0.0.1",
            port=0,
            total_n=n,
            chunk_size=chunk_size,
            lease_timeout_sec=5.0,
            verbose=False,
        )
        master_thread = threading.Thread(target=master.start, daemon=True)
        master_thread.start()
        self.assertTrue(master.wait_ready(timeout=5.0))

        try:
            workers = [
                WorkerNode(
                    worker_id=f"w-{i}",
                    master_host="127.0.0.1",
                    master_port=master.port,
                    heartbeat_interval_sec=0.05,
                    retry_interval_sec=0.05,
                    verbose=False,
                )
                for i in range(3)
            ]
            worker_threads = [
                threading.Thread(target=w.run, daemon=True) for w in workers
            ]
            for wt in worker_threads:
                wt.start()

            self.assertTrue(master.wait_until_finished(timeout=15.0))
            for wt in worker_threads:
                wt.join(timeout=5.0)
                self.assertFalse(wt.is_alive())

            summary = master.task_manager.get_summary()
            self.assertEqual(summary["total_sum"], baseline.prime_sum)
            self.assertEqual(summary["total_primes"], baseline.primes_count)
            self.assertEqual(sum(w.tasks_completed for w in workers), summary["total_tasks"])
        finally:
            master.stop()
            master_thread.join(timeout=3.0)

    def test_master_with_small_n_and_no_task_shutdown(self) -> None:
        master = MasterNode(
            host="127.0.0.1",
            port=0,
            total_n=1,
            chunk_size=10,
            lease_timeout_sec=2.0,
            verbose=False,
        )
        master_thread = threading.Thread(target=master.start, daemon=True)
        master_thread.start()
        self.assertTrue(master.wait_ready(timeout=5.0))

        try:
            worker = WorkerNode(
                worker_id="w-empty",
                master_host="127.0.0.1",
                master_port=master.port,
                verbose=False,
            )
            worker.run()
            self.assertEqual(worker.tasks_completed, 0)
            self.assertEqual(master.task_manager.total_sum, 0)
        finally:
            master.stop()
            master_thread.join(timeout=3.0)

    def test_get_task_before_register_is_rejected(self) -> None:
        master = MasterNode(
            host="127.0.0.1",
            port=0,
            total_n=100,
            chunk_size=50,
            lease_timeout_sec=5.0,
            verbose=False,
        )
        master_thread = threading.Thread(target=master.start, daemon=True)
        master_thread.start()
        self.assertTrue(master.wait_ready(timeout=5.0))

        try:
            with socket.create_connection(("127.0.0.1", master.port), timeout=2.0) as sock:
                sock.sendall(encode_message(MessageType.GET_TASK, worker_id="rogue"))
                data = sock.recv(4096)
                self.assertEqual(data, b"")

            self.assertEqual(master.task_manager.get_task(0).state, TaskState.PENDING)  # type: ignore[union-attr]
        finally:
            master.stop()
            master_thread.join(timeout=3.0)

    def test_duplicate_worker_id_disconnect_does_not_reclaim_new_session(self) -> None:
        master = MasterNode(
            host="127.0.0.1",
            port=0,
            total_n=100,
            chunk_size=30,
            lease_timeout_sec=10.0,
            verbose=False,
        )
        master_thread = threading.Thread(target=master.start, daemon=True)
        master_thread.start()
        self.assertTrue(master.wait_ready(timeout=5.0))

        try:
            s1 = socket.create_connection(("127.0.0.1", master.port), timeout=2.0)
            s2 = socket.create_connection(("127.0.0.1", master.port), timeout=2.0)
            try:
                buf1, buf2 = LineBuffer(), LineBuffer()
                s1.sendall(encode_message(MessageType.REGISTER, worker_id="dup-w"))
                self.assertEqual(buf1.feed(s1.recv(4096))[0]["type"], "REGISTERED")

                s2.sendall(encode_message(MessageType.REGISTER, worker_id="dup-w"))
                self.assertEqual(buf2.feed(s2.recv(4096))[0]["type"], "REGISTERED")

                s1.sendall(encode_message(MessageType.GET_TASK, worker_id="dup-w"))
                t1 = buf1.feed(s1.recv(4096))[0]
                self.assertEqual(t1["task_id"], 0)

                s2.sendall(encode_message(MessageType.GET_TASK, worker_id="dup-w"))
                t2 = buf2.feed(s2.recv(4096))[0]
                self.assertEqual(t2["task_id"], 1)

                # Fecha conexão antiga s1; apenas tarefa 0 deve voltar para PENDING
                s1.close()
                threading.Event().wait(0.15)
                self.assertEqual(master.task_manager.get_task(0).state, TaskState.PENDING)  # type: ignore[union-attr]
                self.assertEqual(master.task_manager.get_task(1).state, TaskState.IN_PROGRESS)  # type: ignore[union-attr]
            finally:
                s1.close()
                s2.close()
        finally:
            master.stop()
            master_thread.join(timeout=3.0)

    def test_worker_recv_next_respects_stop_on_socket_timeout(self) -> None:
        worker = WorkerNode(worker_id="w-timeout", verbose=False)

        class TimeoutSocket:
            def __init__(self) -> None:
                self.calls = 0

            def recv(self, _bufsize: int) -> bytes:
                self.calls += 1
                if self.calls >= 2:
                    worker.stop()
                raise socket.timeout("timed out")

        msg = worker._recv_next(TimeoutSocket(), LineBuffer())  # type: ignore[arg-type]
        self.assertIsNone(msg)
        self.assertTrue(worker._stop_event.is_set())

    def test_worker_retries_connection_until_master_starts(self) -> None:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        worker = WorkerNode(
            worker_id="w-retry",
            master_host="127.0.0.1",
            master_port=port,
            connect_retry_interval_sec=0.05,
            verbose=False,
        )
        wt = threading.Thread(target=worker.run, daemon=True)
        wt.start()

        threading.Event().wait(0.15)
        master = MasterNode(
            host="127.0.0.1",
            port=port,
            total_n=100,
            chunk_size=50,
            lease_timeout_sec=5.0,
            verbose=False,
        )
        master_thread = threading.Thread(target=master.start, daemon=True)
        master_thread.start()
        self.assertTrue(master.wait_ready(timeout=5.0))

        try:
            self.assertTrue(master.wait_until_finished(timeout=5.0))
            wt.join(timeout=5.0)
            self.assertFalse(wt.is_alive())
            self.assertEqual(worker.tasks_completed, 2)
        finally:
            worker.stop()
            master.stop()
            master_thread.join(timeout=3.0)

    def test_worker_raises_when_connect_retries_exhausted(self) -> None:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        worker = WorkerNode(
            worker_id="w-fail",
            master_host="127.0.0.1",
            master_port=port,
            connect_retry_interval_sec=0.02,
            connect_retries=1,
            verbose=False,
        )
        with self.assertRaises(OSError):
            worker.run()


if __name__ == "__main__":
    unittest.main()
