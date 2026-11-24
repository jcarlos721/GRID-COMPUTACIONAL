# Simulação reproduzível de falhas: queda abrupta de worker e atraso com expiração de lease.

from __future__ import annotations

import threading
import time
import unittest

from src.master import MasterNode
from src.sequential_baseline import run_sequential_baseline
from src.worker import WorkerNode


class TestFaultSimulation(unittest.TestCase):
    def test_abrupt_worker_crash_reclaims_and_completes(self) -> None:
        """Worker cai abruptamente no meio da execução; Master reatribui e conclui com soma exata."""
        n = 50_000
        chunk_size = 5_000
        baseline = run_sequential_baseline(n)

        master = MasterNode(
            host="127.0.0.1",
            port=0,
            total_n=n,
            chunk_size=chunk_size,
            lease_timeout_sec=1.0,
            verbose=False,
        )
        master_thread = threading.Thread(target=master.start, daemon=True)
        master_thread.start()
        self.assertTrue(master.wait_ready(timeout=5.0))

        try:
            w_crash = WorkerNode(
                worker_id="w-crash",
                master_host="127.0.0.1",
                master_port=master.port,
                fail_after_tasks=1,
                heartbeat_interval_sec=0.1,
                retry_interval_sec=0.05,
                verbose=False,
            )
            w_healthy = WorkerNode(
                worker_id="w-healthy",
                master_host="127.0.0.1",
                master_port=master.port,
                heartbeat_interval_sec=0.1,
                retry_interval_sec=0.05,
                verbose=False,
            )

            t_crash = threading.Thread(target=w_crash.run, daemon=True)
            t_healthy = threading.Thread(target=w_healthy.run, daemon=True)
            t_crash.start()
            t_healthy.start()

            t_crash.join(timeout=5.0)
            self.assertFalse(t_crash.is_alive())
            self.assertEqual(w_crash.tasks_completed, 1)

            self.assertTrue(master.wait_until_finished(timeout=10.0))
            t_healthy.join(timeout=5.0)
            self.assertFalse(t_healthy.is_alive())

            summary = master.task_manager.get_summary()
            self.assertEqual(summary["total_sum"], baseline.prime_sum)
            self.assertEqual(summary["total_primes"], baseline.primes_count)
            self.assertGreaterEqual(summary["reassignments"], 1)
            self.assertEqual(
                w_crash.tasks_completed + w_healthy.tasks_completed,
                summary["total_tasks"],
            )
        finally:
            master.stop()
            master_thread.join(timeout=3.0)

    def test_slow_worker_triggers_lease_expiration_and_stale_reject(self) -> None:
        """Worker lento responde após lease expirar; Master rejeita STALE_ATTEMPT sem dupla contagem."""
        n = 40_000
        chunk_size = 10_000
        baseline = run_sequential_baseline(n)

        master = MasterNode(
            host="127.0.0.1",
            port=0,
            total_n=n,
            chunk_size=chunk_size,
            lease_timeout_sec=0.35,
            verbose=False,
        )
        master_thread = threading.Thread(target=master.start, daemon=True)
        master_thread.start()
        self.assertTrue(master.wait_ready(timeout=5.0))

        try:
            w_slow = WorkerNode(
                worker_id="w-slow",
                master_host="127.0.0.1",
                master_port=master.port,
                artificial_delay_sec=0.9,
                heartbeat_interval_sec=0.1,
                retry_interval_sec=0.05,
                verbose=False,
            )
            w_fast = WorkerNode(
                worker_id="w-fast",
                master_host="127.0.0.1",
                master_port=master.port,
                heartbeat_interval_sec=0.1,
                retry_interval_sec=0.05,
                verbose=False,
            )

            t_slow = threading.Thread(target=w_slow.run, daemon=True)
            t_slow.start()
            # Garante que w-slow pegue a primeira tarefa antes do w-fast iniciar
            time.sleep(0.1)

            t_fast = threading.Thread(target=w_fast.run, daemon=True)
            t_fast.start()

            self.assertTrue(master.wait_until_finished(timeout=10.0))
            t_fast.join(timeout=5.0)
            t_slow.join(timeout=5.0)
            self.assertFalse(t_fast.is_alive())
            self.assertFalse(t_slow.is_alive())

            self.assertIn("STALE_ATTEMPT", w_slow.rejected_reasons)
            summary = master.task_manager.get_summary()
            self.assertEqual(summary["total_sum"], baseline.prime_sum)
            self.assertEqual(summary["total_primes"], baseline.primes_count)
            self.assertGreaterEqual(summary["reassignments"], 1)
            self.assertGreaterEqual(summary["stale_rejects"], 1)
        finally:
            master.stop()
            master_thread.join(timeout=3.0)


if __name__ == "__main__":
    unittest.main()
