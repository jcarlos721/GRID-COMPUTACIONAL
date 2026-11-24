# Testes unitários isolados para protocolo, crivo segmentado, baseline e máquina de estados.

from __future__ import annotations

import math
import threading
import unittest

from src.protocol import (
    LineBuffer,
    MessageType,
    ProtocolError,
    decode_message,
    encode_message,
)
from src.sequential_baseline import run_sequential_baseline
from src.sieve import generate_base_primes, sum_primes_in_range
from src.task_manager import TaskManager, TaskState


class TestProtocol(unittest.TestCase):
    def test_encode_decode_all_message_types(self) -> None:
        samples = [
            (MessageType.REGISTER, {"worker_id": "w1"}),
            (
                MessageType.REGISTERED,
                {"worker_id": "w1", "base_primes": [2, 3, 5], "lease_timeout_sec": 5.0},
            ),
            (MessageType.GET_TASK, {"worker_id": "w1"}),
            (
                MessageType.TASK_ASSIGN,
                {
                    "task_id": 0,
                    "attempt_id": 1,
                    "start": 2,
                    "end": 100,
                    "deadline_ts": 1234.5,
                },
            ),
            (MessageType.HEARTBEAT, {"worker_id": "w1", "task_id": 0, "attempt_id": 1}),
            (
                MessageType.SUBMIT_RESULT,
                {
                    "worker_id": "w1",
                    "task_id": 0,
                    "attempt_id": 1,
                    "partial_sum": 1060,
                    "primes_count": 25,
                    "elapsed_ms": 1.25,
                },
            ),
            (MessageType.NO_TASK, {}),
            (MessageType.ACK, {"task_id": 0, "attempt_id": 1, "accepted": True}),
            (
                MessageType.REJECT,
                {
                    "task_id": 0,
                    "attempt_id": 1,
                    "accepted": False,
                    "reason": "STALE_ATTEMPT",
                },
            ),
        ]

        for msg_type, payload in samples:
            encoded = encode_message(msg_type, **payload)
            self.assertTrue(encoded.endswith(b"\n"))
            decoded = decode_message(encoded)
            self.assertEqual(decoded["type"], msg_type.value)
            for key, value in payload.items():
                self.assertEqual(decoded[key], value)

    def test_line_buffer_handles_partial_and_batched_chunks(self) -> None:
        msg1 = encode_message(MessageType.REGISTER, worker_id="worker-a")
        msg2 = encode_message(MessageType.GET_TASK, worker_id="worker-a")
        stream = msg1 + b"\n" + msg2

        buf = LineBuffer()
        part1 = stream[:7]
        part2 = stream[7 : len(msg1) + 3]
        part3 = stream[len(msg1) + 3 :]

        self.assertEqual(buf.feed(part1), [])
        self.assertTrue(buf.has_pending_data())

        out2 = buf.feed(part2)
        self.assertEqual(len(out2), 1)
        self.assertEqual(out2[0]["type"], "REGISTER")
        self.assertTrue(buf.has_pending_data())

        out3 = buf.feed(part3)
        self.assertEqual(len(out3), 1)
        self.assertEqual(out3[0]["type"], "GET_TASK")
        self.assertFalse(buf.has_pending_data())

    def test_protocol_validation_errors(self) -> None:
        with self.assertRaises(ProtocolError):
            decode_message(b"{invalid json\n")

        with self.assertRaises(ProtocolError):
            decode_message(b'{"type":"UNKNOWN"}\n')

        with self.assertRaises(ProtocolError):
            encode_message(MessageType.TASK_ASSIGN, task_id=0, attempt_id=1)

        with self.assertRaises(ProtocolError):
            encode_message(MessageType.ACK, task_id=0, attempt_id=1, accepted=False)

        with self.assertRaises(ProtocolError):
            encode_message(
                MessageType.TASK_ASSIGN,
                task_id=True,  # bool não deve ser aceito como int
                attempt_id=1,
                start=2,
                end=10,
                deadline_ts=10.0,
            )

        with self.assertRaises(ProtocolError):
            encode_message(
                MessageType.SUBMIT_RESULT,
                worker_id="w1",
                task_id=0,
                attempt_id=1,
                partial_sum=-1,
                primes_count=0,
                elapsed_ms=1.0,
            )

        with self.assertRaises(ProtocolError):
            encode_message(
                MessageType.SUBMIT_RESULT,
                worker_id="w1",
                task_id=0,
                attempt_id=1,
                partial_sum=10,
                primes_count=-2,
                elapsed_ms=1.0,
            )


class TestSieveAndBaseline(unittest.TestCase):
    def test_generate_base_primes(self) -> None:
        self.assertEqual(generate_base_primes(-5), [])
        self.assertEqual(generate_base_primes(1), [])
        self.assertEqual(generate_base_primes(2), [2])
        self.assertEqual(generate_base_primes(30), [2, 3, 5, 7, 11, 13, 17, 19, 23, 29])

    def test_range_edge_cases(self) -> None:
        self.assertEqual(sum_primes_in_range(-10, 1), (0, 0))
        self.assertEqual(sum_primes_in_range(10, 5), (0, 0))
        self.assertEqual(sum_primes_in_range(0, 10), (17, 4))  # 2, 3, 5, 7
        self.assertEqual(sum_primes_in_range(2, 2), (2, 1))
        self.assertEqual(sum_primes_in_range(4, 4), (0, 0))

    def test_contiguous_subintervals_match_sequential_baseline(self) -> None:
        cases = [
            (0, 10),
            (1, 10),
            (2, 1),
            (10, 3),
            (100, 17),
            (10_000, 777),
            (50_000, 4_096),
        ]
        for n, chunk_size in cases:
            baseline = run_sequential_baseline(n, segment_size=1_000)
            base_primes = generate_base_primes(math.isqrt(max(0, n)))

            total_sum = 0
            total_count = 0
            if n >= 2:
                current = 2
                while current <= n:
                    end = min(current + chunk_size - 1, n)
                    part_sum, part_count = sum_primes_in_range(
                        current, end, base_primes=base_primes
                    )
                    total_sum += part_sum
                    total_count += part_count
                    current = end + 1

            self.assertEqual(total_sum, baseline.prime_sum, msg=f"N={n}, chunk={chunk_size}")
            self.assertEqual(
                total_count, baseline.primes_count, msg=f"N={n}, chunk={chunk_size}"
            )


class TestTaskManager(unittest.TestCase):
    def test_state_transitions_and_lease_expiration(self) -> None:
        tm = TaskManager(total_n=20, chunk_size=10, lease_timeout_sec=5.0)
        # [2, 11] e [12, 20]
        self.assertEqual(tm.get_summary()["total_tasks"], 2)

        a1 = tm.assign_task("w1", now=100.0)
        self.assertIsNotNone(a1)
        assert a1 is not None
        self.assertEqual(a1.task_id, 0)
        self.assertEqual(a1.attempt_id, 1)
        self.assertEqual(a1.deadline_ts, 105.0)
        self.assertEqual(tm.get_task(0).state, TaskState.IN_PROGRESS)  # type: ignore[union-attr]

        # Heartbeat antes de expirar renova o lease
        self.assertTrue(tm.renew_lease("w1", a1.task_id, a1.attempt_id, now=104.0))
        self.assertEqual(tm.get_task(0).deadline_ts, 109.0)  # type: ignore[union-attr]

        # Expiração do lease retorna tarefa para PENDING
        reclaimed = tm.reclaim_expired_tasks(now=109.0)
        self.assertEqual(reclaimed, [0])
        self.assertEqual(tm.get_task(0).state, TaskState.PENDING)  # type: ignore[union-attr]

        # Nova atribuição gera novo attempt_id
        a2 = tm.assign_task("w2", now=110.0)
        self.assertIsNotNone(a2)
        assert a2 is not None
        self.assertEqual(a2.task_id, 0)
        self.assertEqual(a2.attempt_id, 2)

        # Submissão obsoleta do w1 (attempt_id=1) deve ser rejeitada sem alterar soma
        stale = tm.submit_result("w1", a1.task_id, a1.attempt_id, 17, 4, 1.0)
        self.assertFalse(stale.accepted)
        self.assertEqual(stale.reason, "STALE_ATTEMPT")
        self.assertEqual(tm.total_sum, 0)

        # Submissão válida do w2 (attempt_id=2) conclui a tarefa
        part_sum, part_count = sum_primes_in_range(a2.start, a2.end)
        ok = tm.submit_result("w2", a2.task_id, a2.attempt_id, part_sum, part_count, 2.0)
        self.assertTrue(ok.accepted)
        self.assertEqual(tm.get_task(0).state, TaskState.COMPLETED)  # type: ignore[union-attr]
        self.assertEqual(tm.total_sum, part_sum)

        # Reenvio duplicado da tarefa já concluída deve ser rejeitado sem dupla contagem
        dup = tm.submit_result("w2", a2.task_id, a2.attempt_id, part_sum, part_count, 2.0)
        self.assertFalse(dup.accepted)
        self.assertEqual(dup.reason, "ALREADY_COMPLETED")
        self.assertEqual(tm.total_sum, part_sum)

        # Falha explícita libera tarefa IN_PROGRESS para PENDING
        a3 = tm.assign_task("w1", now=111.0)
        self.assertIsNotNone(a3)
        assert a3 is not None
        self.assertEqual(a3.task_id, 1)
        self.assertTrue(tm.fail_task("w1", a3.task_id, a3.attempt_id))
        self.assertEqual(tm.get_task(1).state, TaskState.PENDING)  # type: ignore[union-attr]

    def test_concurrent_workers_match_baseline(self) -> None:
        n = 25_000
        tm = TaskManager(total_n=n, chunk_size=1_000, lease_timeout_sec=30.0)
        base_primes = generate_base_primes(math.isqrt(n))
        errors: list[Exception] = []

        def worker_loop(worker_id: str) -> None:
            try:
                while True:
                    assignment = tm.assign_task(worker_id)
                    if assignment is None:
                        break
                    part_sum, part_count = sum_primes_in_range(
                        assignment.start, assignment.end, base_primes=base_primes
                    )
                    outcome = tm.submit_result(
                        worker_id=worker_id,
                        task_id=assignment.task_id,
                        attempt_id=assignment.attempt_id,
                        partial_sum=part_sum,
                        primes_count=part_count,
                        elapsed_ms=0.1,
                    )
                    if not outcome.accepted:
                        raise AssertionError(f"Submissão rejeitada: {outcome}")
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [
            threading.Thread(target=worker_loop, args=(f"worker-{i}",))
            for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertTrue(tm.is_finished())
        baseline = run_sequential_baseline(n)
        self.assertEqual(tm.total_sum, baseline.prime_sum)
        self.assertEqual(tm.total_primes, baseline.primes_count)

    def test_reclaim_worker_tasks_isolated_by_session_id(self) -> None:
        tm = TaskManager(total_n=30, chunk_size=10, lease_timeout_sec=10.0)
        a1 = tm.assign_task("w1", now=100.0, session_id="sess-old")
        a2 = tm.assign_task("w1", now=100.0, session_id="sess-new")
        assert a1 is not None and a2 is not None

        reclaimed = tm.reclaim_worker_tasks("w1", session_id="sess-old")
        self.assertEqual(reclaimed, [a1.task_id])
        self.assertEqual(tm.get_task(a1.task_id).state, TaskState.PENDING)  # type: ignore[union-attr]
        self.assertEqual(tm.get_task(a2.task_id).state, TaskState.IN_PROGRESS)  # type: ignore[union-attr]


if __name__ == "__main__":
    unittest.main()
