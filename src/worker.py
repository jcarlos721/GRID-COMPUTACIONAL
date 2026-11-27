# Nó trabalhador (Worker) do Grid Computacional sobre TCP com modelo Pull e Heartbeat.

from __future__ import annotations

import argparse
from collections import deque
import os
import socket
import sys
import threading
import time
import uuid
from typing import Any

from src.protocol import (
    LineBuffer,
    MessageType,
    ProtocolError,
    encode_message,
)
from src.sieve import sum_primes_in_range


class WorkerNode:
    """Cliente TCP responsável por registrar-se no Master, processar tarefas e enviar heartbeats."""

    def __init__(
        self,
        worker_id: str | None = None,
        master_host: str = "127.0.0.1",
        master_port: int = 5000,
        heartbeat_interval_sec: float | None = None,
        retry_interval_sec: float = 0.25,
        connect_retry_interval_sec: float = 2.0,
        connect_retries: int | None = None,
        artificial_delay_sec: float = 0.0,
        fail_after_tasks: int | None = None,
        verbose: bool = True,
    ) -> None:
        self.worker_id = worker_id or f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.master_host = master_host
        self.master_port = master_port
        self.heartbeat_interval_sec = heartbeat_interval_sec
        self.retry_interval_sec = retry_interval_sec
        self.connect_retry_interval_sec = connect_retry_interval_sec
        self.connect_retries = connect_retries
        self.artificial_delay_sec = artificial_delay_sec
        self.fail_after_tasks = fail_after_tasks
        self.verbose = verbose

        self.base_primes: list[int] = []
        self.lease_timeout_sec: float = 10.0
        self.tasks_completed: int = 0
        self.rejected_reasons: list[str] = []
        self._stop_event = threading.Event()
        self._send_lock = threading.Lock()
        self._pending_messages: deque[dict[str, Any]] = deque()

    def _log(self, message: str) -> None:
        if self.verbose:
            print(f"[WORKER:{self.worker_id}] {message}", flush=True)

    def _send(self, sock: socket.socket, msg_type: MessageType, **payload: Any) -> None:
        data = encode_message(msg_type, **payload)
        with self._send_lock:
            sock.sendall(data)

    def _recv_next(self, sock: socket.socket, buffer: LineBuffer) -> dict[str, Any] | None:
        if self._pending_messages:
            return self._pending_messages.popleft()
        while not self._stop_event.is_set():
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                return None
            messages = buffer.feed(chunk)
            if messages:
                self._pending_messages.extend(messages[1:])
                return messages[0]
        return None

    def _heartbeat_loop(
        self,
        sock: socket.socket,
        task_id: int,
        attempt_id: int,
        interval_sec: float,
        done_event: threading.Event,
    ) -> None:
        while not done_event.wait(interval_sec):
            if self._stop_event.is_set():
                break
            try:
                self._send(
                    sock,
                    MessageType.HEARTBEAT,
                    worker_id=self.worker_id,
                    task_id=task_id,
                    attempt_id=attempt_id,
                )
            except OSError:
                break

    def stop(self) -> None:
        """Sinaliza parada limpa do Worker."""
        self._stop_event.set()

    def _connect_to_master(self) -> socket.socket | None:
        """Conecta ao Master com retentativas enquanto ele não estiver disponível."""
        attempts = 0
        while not self._stop_event.is_set():
            try:
                return socket.create_connection(
                    (self.master_host, self.master_port), timeout=10.0
                )
            except OSError as exc:
                if (
                    self.connect_retries is not None
                    and attempts >= self.connect_retries
                ):
                    raise exc
                attempts += 1
                self._log(
                    f"Master em {self.master_host}:{self.master_port} indisponível "
                    f"({exc}). Tentando novamente em {self.connect_retry_interval_sec:.1f}s..."
                )
                if self._stop_event.wait(self.connect_retry_interval_sec):
                    return None
        return None

    def run(self) -> None:
        """Executa o loop principal do Worker conectando ao Master via TCP."""
        buffer = LineBuffer()
        sock = self._connect_to_master()
        if sock is None:
            return
        with sock:
            sock.settimeout(1.0)
            self._send(sock, MessageType.REGISTER, worker_id=self.worker_id)

            reg_msg = self._recv_next(sock, buffer)
            if reg_msg is None or reg_msg["type"] != MessageType.REGISTERED.value:
                raise ProtocolError("Resposta REGISTERED esperada após REGISTER.")

            self.base_primes = list(reg_msg["base_primes"])
            self.lease_timeout_sec = float(reg_msg["lease_timeout_sec"])
            hb_interval = (
                self.heartbeat_interval_sec
                if self.heartbeat_interval_sec is not None
                else max(0.2, self.lease_timeout_sec / 3.0)
            )
            self._log(
                f"Registrado no Master {self.master_host}:{self.master_port} | "
                f"base_primes={len(self.base_primes)} | hb={hb_interval:.2f}s"
            )

            while not self._stop_event.is_set():
                self._send(sock, MessageType.GET_TASK, worker_id=self.worker_id)
                reply = self._recv_next(sock, buffer)
                if reply is None:
                    self._log("Conexão encerrada pelo Master.")
                    break

                reply_type = MessageType(reply["type"])
                if reply_type == MessageType.NO_TASK:
                    if reply.get("done"):
                        self._log("Todas as tarefas concluídas. Encerrando.")
                        break
                    wait_sec = float(reply.get("retry_after_sec", self.retry_interval_sec))
                    self._stop_event.wait(wait_sec)
                    continue

                if reply_type != MessageType.TASK_ASSIGN:
                    raise ProtocolError(f"Resposta inesperada para GET_TASK: {reply_type}")

                task_id = int(reply["task_id"])
                attempt_id = int(reply["attempt_id"])
                start = int(reply["start"])
                end = int(reply["end"])

                if (
                    self.fail_after_tasks is not None
                    and self.tasks_completed >= self.fail_after_tasks
                ):
                    self._log(
                        f"Simulando queda abrupta após {self.tasks_completed} tarefas "
                        f"(task={task_id}, attempt={attempt_id})."
                    )
                    break

                done_event = threading.Event()
                hb_thread = threading.Thread(
                    target=self._heartbeat_loop,
                    args=(sock, task_id, attempt_id, hb_interval, done_event),
                    daemon=True,
                )
                hb_thread.start()

                t0 = time.perf_counter()
                try:
                    partial_sum, primes_count = sum_primes_in_range(
                        start, end, base_primes=self.base_primes
                    )
                finally:
                    done_event.set()
                    hb_thread.join(timeout=1.0)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                if self.artificial_delay_sec > 0:
                    self._log(
                        f"Atraso artificial de {self.artificial_delay_sec:.2f}s antes de enviar "
                        f"task={task_id} (attempt={attempt_id})."
                    )
                    if self._stop_event.wait(self.artificial_delay_sec):
                        break

                self._send(
                    sock,
                    MessageType.SUBMIT_RESULT,
                    worker_id=self.worker_id,
                    task_id=task_id,
                    attempt_id=attempt_id,
                    partial_sum=partial_sum,
                    primes_count=primes_count,
                    elapsed_ms=elapsed_ms,
                )

                ack = self._recv_next(sock, buffer)
                if ack is None:
                    self._log("Conexão encerrada após SUBMIT_RESULT.")
                    break
                if ack["type"] == MessageType.ACK.value:
                    self.tasks_completed += 1
                    self._log(
                        f"Task {task_id} aceita (sum={partial_sum}, count={primes_count}, "
                        f"{elapsed_ms:.2f}ms)"
                    )
                else:
                    reason = str(ack.get("reason", "UNKNOWN"))
                    self.rejected_reasons.append(reason)
                    self._log(
                        f"Task {task_id} rejeitada: {reason}"
                    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Worker do Grid Computacional")
    parser.add_argument("--master-host", default="127.0.0.1", help="Host do Master")
    parser.add_argument("--master-port", type=int, default=5000, help="Porta do Master")
    parser.add_argument("--worker-id", default=None, help="Identificador do Worker")
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=None,
        help="Intervalo de heartbeat em segundos",
    )
    parser.add_argument(
        "--artificial-delay",
        type=float,
        default=0.0,
        help="Atraso artificial em segundos antes de enviar SUBMIT_RESULT (simula nó lento)",
    )
    parser.add_argument(
        "--fail-after-tasks",
        type=int,
        default=None,
        help="Encerra o worker abruptamente após concluir N tarefas (simula queda)",
    )
    parser.add_argument(
        "--connect-retries",
        type=int,
        default=None,
        help="Número máximo de tentativas de conexão ao Master (padrão: aguarda indefinidamente)",
    )
    parser.add_argument(
        "--connect-retry-interval",
        type=float,
        default=2.0,
        help="Intervalo em segundos entre tentativas de conexão ao Master",
    )
    args = parser.parse_args()

    worker = WorkerNode(
        worker_id=args.worker_id,
        master_host=args.master_host,
        master_port=args.master_port,
        heartbeat_interval_sec=args.heartbeat_interval,
        connect_retry_interval_sec=args.connect_retry_interval,
        connect_retries=args.connect_retries,
        artificial_delay_sec=args.artificial_delay,
        fail_after_tasks=args.fail_after_tasks,
    )
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()
    except OSError as exc:
        print(
            f"[WORKER:{worker.worker_id}] Não foi possível conectar ao Master em "
            f"{worker.master_host}:{worker.master_port}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
