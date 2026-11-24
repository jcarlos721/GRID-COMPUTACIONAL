# Máquina de estados das tarefas do Grid com controle de lease e proteção contra resultados obsoletos.

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, replace
from enum import Enum


class TaskState(str, Enum):
    """Estados possíveis de uma tarefa no TaskManager."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"


@dataclass
class Task:
    """Representa um subintervalo `[start, end]` a ser processado por um worker."""

    task_id: int
    start: int
    end: int
    state: TaskState = TaskState.PENDING
    attempt_id: int = 0
    worker_id: str | None = None
    session_id: str | None = None
    deadline_ts: float | None = None
    partial_sum: int = 0
    primes_count: int = 0
    elapsed_ms: float = 0.0


@dataclass(frozen=True)
class TaskAssignment:
    """Snapshot imutável enviado ao worker quando uma tarefa é atribuída."""

    task_id: int
    attempt_id: int
    start: int
    end: int
    deadline_ts: float


@dataclass(frozen=True)
class SubmitResultOutcome:
    """Resultado da tentativa de submissão de uma tarefa concluída."""

    accepted: bool
    task_id: int
    attempt_id: int
    reason: str | None = None


class TaskManager:
    """Gerencia o ciclo de vida das tarefas com concorrência thread-safe."""

    def __init__(
        self,
        total_n: int,
        chunk_size: int,
        lease_timeout_sec: float = 10.0,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size deve ser maior que zero.")
        if lease_timeout_sec <= 0:
            raise ValueError("lease_timeout_sec deve ser maior que zero.")

        self._lock = threading.Lock()
        self._lease_timeout_sec = float(lease_timeout_sec)
        self._tasks: dict[int, Task] = {}
        self._total_sum = 0
        self._total_primes = 0
        self._reassignments = 0
        self._stale_rejects = 0
        self._completed_by_worker: dict[str, int] = {}

        if total_n >= 2:
            task_id = 0
            current = 2
            while current <= total_n:
                end = min(current + chunk_size - 1, total_n)
                self._tasks[task_id] = Task(task_id=task_id, start=current, end=end)
                task_id += 1
                current = end + 1

    @property
    def lease_timeout_sec(self) -> float:
        return self._lease_timeout_sec

    @property
    def total_sum(self) -> int:
        with self._lock:
            return self._total_sum

    @property
    def total_primes(self) -> int:
        with self._lock:
            return self._total_primes

    def _reclaim_expired_locked(self, now: float) -> list[int]:
        reclaimed: list[int] = []
        for task in self._tasks.values():
            if (
                task.state == TaskState.IN_PROGRESS
                and task.deadline_ts is not None
                and now >= task.deadline_ts
            ):
                task.state = TaskState.PENDING
                task.worker_id = None
                task.session_id = None
                task.deadline_ts = None
                self._reassignments += 1
                reclaimed.append(task.task_id)
        return reclaimed

    def reclaim_expired_tasks(self, now: float | None = None) -> list[int]:
        """Reverte tarefas cujo lease expirou de IN_PROGRESS para PENDING."""
        current_time = time.monotonic() if now is None else now
        with self._lock:
            return self._reclaim_expired_locked(current_time)

    def reclaim_worker_tasks(
        self,
        worker_id: str,
        session_id: str | None = None,
    ) -> list[int]:
        """Reverte para PENDING tarefas IN_PROGRESS associadas à sessão/worker desconectado."""
        reclaimed: list[int] = []
        with self._lock:
            for task in self._tasks.values():
                if (
                    task.state == TaskState.IN_PROGRESS
                    and task.worker_id == worker_id
                    and (session_id is None or task.session_id == session_id)
                ):
                    task.state = TaskState.PENDING
                    task.worker_id = None
                    task.session_id = None
                    task.deadline_ts = None
                    self._reassignments += 1
                    reclaimed.append(task.task_id)
        return reclaimed

    def assign_task(
        self,
        worker_id: str,
        now: float | None = None,
        session_id: str | None = None,
    ) -> TaskAssignment | None:
        """Atribui a próxima tarefa PENDING para `worker_id`, gerando novo `attempt_id`."""
        current_time = time.monotonic() if now is None else now
        with self._lock:
            self._reclaim_expired_locked(current_time)
            for task in self._tasks.values():
                if task.state == TaskState.PENDING:
                    task.state = TaskState.IN_PROGRESS
                    task.attempt_id += 1
                    task.worker_id = worker_id
                    task.session_id = session_id
                    task.deadline_ts = current_time + self._lease_timeout_sec
                    return TaskAssignment(
                        task_id=task.task_id,
                        attempt_id=task.attempt_id,
                        start=task.start,
                        end=task.end,
                        deadline_ts=task.deadline_ts,
                    )
        return None

    def renew_lease(
        self,
        worker_id: str,
        task_id: int,
        attempt_id: int,
        now: float | None = None,
    ) -> bool:
        """Renova o lease de uma tarefa IN_PROGRESS se worker_id e attempt_id coincidirem."""
        current_time = time.monotonic() if now is None else now
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if (
                task.state != TaskState.IN_PROGRESS
                or task.attempt_id != attempt_id
                or task.worker_id != worker_id
            ):
                return False
            if task.deadline_ts is not None and current_time >= task.deadline_ts:
                return False
            task.deadline_ts = current_time + self._lease_timeout_sec
            return True

    def fail_task(self, worker_id: str, task_id: int, attempt_id: int) -> bool:
        """Libera explicitamente uma tarefa IN_PROGRESS de volta para PENDING."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if (
                task.state != TaskState.IN_PROGRESS
                or task.attempt_id != attempt_id
                or task.worker_id != worker_id
            ):
                return False
            task.state = TaskState.PENDING
            task.worker_id = None
            task.session_id = None
            task.deadline_ts = None
            self._reassignments += 1
            return True

    def submit_result(
        self,
        worker_id: str,
        task_id: int,
        attempt_id: int,
        partial_sum: int,
        primes_count: int,
        elapsed_ms: float,
    ) -> SubmitResultOutcome:
        """Consolida resultado apenas se `state == IN_PROGRESS` e `attempt_id` coincidir."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return SubmitResultOutcome(
                    accepted=False,
                    task_id=task_id,
                    attempt_id=attempt_id,
                    reason="UNKNOWN_TASK",
                )

            if task.attempt_id != attempt_id:
                self._stale_rejects += 1
                return SubmitResultOutcome(
                    accepted=False,
                    task_id=task_id,
                    attempt_id=attempt_id,
                    reason="STALE_ATTEMPT",
                )

            if task.state == TaskState.COMPLETED:
                self._stale_rejects += 1
                return SubmitResultOutcome(
                    accepted=False,
                    task_id=task_id,
                    attempt_id=attempt_id,
                    reason="ALREADY_COMPLETED",
                )

            if task.state != TaskState.IN_PROGRESS:
                return SubmitResultOutcome(
                    accepted=False,
                    task_id=task_id,
                    attempt_id=attempt_id,
                    reason="NOT_IN_PROGRESS",
                )

            if task.worker_id != worker_id:
                return SubmitResultOutcome(
                    accepted=False,
                    task_id=task_id,
                    attempt_id=attempt_id,
                    reason="WORKER_MISMATCH",
                )

            task.state = TaskState.COMPLETED
            task.partial_sum = partial_sum
            task.primes_count = primes_count
            task.elapsed_ms = elapsed_ms
            task.deadline_ts = None
            self._total_sum += partial_sum
            self._total_primes += primes_count
            self._completed_by_worker[worker_id] = (
                self._completed_by_worker.get(worker_id, 0) + 1
            )

            return SubmitResultOutcome(
                accepted=True,
                task_id=task_id,
                attempt_id=attempt_id,
            )

    def get_task(self, task_id: int) -> Task | None:
        """Retorna uma cópia segura do estado atual de uma tarefa."""
        with self._lock:
            task = self._tasks.get(task_id)
            return replace(task) if task is not None else None

    def is_finished(self) -> bool:
        """Indica se todas as tarefas atingiram o estado COMPLETED."""
        with self._lock:
            return all(t.state == TaskState.COMPLETED for t in self._tasks.values())

    def get_summary(self) -> dict[str, object]:
        """Retorna contadores agregados e métricas do estado atual do Grid."""
        with self._lock:
            pending = 0
            in_progress = 0
            completed = 0
            for task in self._tasks.values():
                if task.state == TaskState.PENDING:
                    pending += 1
                elif task.state == TaskState.IN_PROGRESS:
                    in_progress += 1
                else:
                    completed += 1
            return {
                "total_tasks": len(self._tasks),
                "pending": pending,
                "in_progress": in_progress,
                "completed": completed,
                "total_sum": self._total_sum,
                "total_primes": self._total_primes,
                "reassignments": self._reassignments,
                "stale_rejects": self._stale_rejects,
                "completed_by_worker": dict(self._completed_by_worker),
            }
