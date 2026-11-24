# Nó coordenador (Master) do Grid Computacional sobre TCP com modelo Pull e Heartbeat.

from __future__ import annotations

import argparse
import math
import socket
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
from src.sieve import generate_base_primes
from src.task_manager import TaskManager


class MasterNode:
    """Coordenador TCP responsável por distribuir intervalos e consolidar resultados."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5000,
        total_n: int = 1_000_000,
        chunk_size: int = 50_000,
        lease_timeout_sec: float = 10.0,
        verbose: bool = True,
    ) -> None:
        self.host = host
        self.port = port
        self.total_n = total_n
        self.chunk_size = chunk_size
        self.lease_timeout_sec = lease_timeout_sec
        self.verbose = verbose

        self.base_primes = generate_base_primes(math.isqrt(max(0, total_n)))
        self.task_manager = TaskManager(
            total_n=total_n,
            chunk_size=chunk_size,
            lease_timeout_sec=lease_timeout_sec,
        )

        self._server_socket: socket.socket | None = None
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._finished_event = threading.Event()
        self._state_lock = threading.Lock()
        self._active_sockets: set[socket.socket] = set()
        self._client_threads: list[threading.Thread] = []
        self._start_ts: float | None = None

        if self.task_manager.is_finished():
            self._finished_event.set()

    def _log(self, message: str) -> None:
        if self.verbose:
            print(f"[MASTER] {message}", flush=True)

    def _send(self, conn: socket.socket, msg_type: MessageType, **payload: Any) -> None:
        data = encode_message(msg_type, **payload)
        conn.sendall(data)

    def _handle_message(
        self,
        conn: socket.socket,
        msg: dict[str, Any],
        registered_worker: str | None,
        session_id: str,
    ) -> str | None:
        msg_type = MessageType(msg["type"])

        if msg_type == MessageType.REGISTER:
            worker_id = msg["worker_id"]
            self._log(f"Worker registrado: {worker_id} (session={session_id[:8]})")
            self._send(
                conn,
                MessageType.REGISTERED,
                worker_id=worker_id,
                base_primes=self.base_primes,
                lease_timeout_sec=self.lease_timeout_sec,
            )
            return worker_id

        if registered_worker is None:
            self._log(
                f"Mensagem {msg_type.value} rejeitada: cliente não completou REGISTER "
                f"(session={session_id[:8]})."
            )
            raise ProtocolError(
                f"REGISTER obrigatório antes de {msg_type.value}."
            )

        if msg_type == MessageType.GET_TASK:
            worker_id = msg["worker_id"]
            assignment = self.task_manager.assign_task(
                worker_id, session_id=session_id
            )
            if assignment is not None:
                self._log(
                    f"Atribuída tarefa {assignment.task_id} (attempt={assignment.attempt_id}) "
                    f"[{assignment.start}, {assignment.end}] -> {worker_id}"
                )
                self._send(
                    conn,
                    MessageType.TASK_ASSIGN,
                    task_id=assignment.task_id,
                    attempt_id=assignment.attempt_id,
                    start=assignment.start,
                    end=assignment.end,
                    deadline_ts=assignment.deadline_ts,
                )
            else:
                done = self.task_manager.is_finished()
                self._send(
                    conn,
                    MessageType.NO_TASK,
                    done=done,
                    retry_after_sec=0.25,
                )
            return registered_worker

        if msg_type == MessageType.HEARTBEAT:
            self.task_manager.renew_lease(
                worker_id=msg["worker_id"],
                task_id=msg["task_id"],
                attempt_id=msg["attempt_id"],
            )
            return registered_worker

        if msg_type == MessageType.SUBMIT_RESULT:
            outcome = self.task_manager.submit_result(
                worker_id=msg["worker_id"],
                task_id=msg["task_id"],
                attempt_id=msg["attempt_id"],
                partial_sum=msg["partial_sum"],
                primes_count=msg["primes_count"],
                elapsed_ms=msg["elapsed_ms"],
            )
            if outcome.accepted:
                summary = self.task_manager.get_summary()
                self._log(
                    f"Tarefa {outcome.task_id} (attempt={outcome.attempt_id}) concluída por "
                    f"{msg['worker_id']} | soma_parcial={msg['partial_sum']} | "
                    f"acumulado={summary['total_sum']} "
                    f"({summary['completed']}/{summary['total_tasks']})"
                )
                self._send(
                    conn,
                    MessageType.ACK,
                    task_id=outcome.task_id,
                    attempt_id=outcome.attempt_id,
                    accepted=True,
                )
                if self.task_manager.is_finished():
                    elapsed_ms = (
                        (time.perf_counter() - self._start_ts) * 1000.0
                        if self._start_ts is not None
                        else 0.0
                    )
                    self._log(
                        f"CONCLUÍDO: N={self.total_n} | primes={summary['total_primes']} | "
                        f"sum={summary['total_sum']} | elapsed_ms={elapsed_ms:.2f}"
                    )
                    self._log(
                        "RESUMO MÉTRICAS | "
                        f"elapsed_sec={elapsed_ms / 1000.0:.3f} | "
                        f"total_tasks={summary['total_tasks']} | "
                        f"reassignments={summary['reassignments']} | "
                        f"stale_rejects={summary['stale_rejects']} | "
                        f"completed_by_worker={summary['completed_by_worker']} | "
                        f"final_sum={summary['total_sum']}"
                    )
                    self._finished_event.set()
            else:
                reason = outcome.reason or "REJECTED"
                self._log(
                    f"Resultado rejeitado: task={outcome.task_id} attempt={outcome.attempt_id} "
                    f"worker={msg['worker_id']} reason={reason}"
                )
                self._send(
                    conn,
                    MessageType.REJECT,
                    task_id=outcome.task_id,
                    attempt_id=outcome.attempt_id,
                    accepted=False,
                    reason=reason,
                )
            return registered_worker

        return registered_worker

    def _handle_client(self, conn: socket.socket, addr: tuple[str, int]) -> None:
        buffer = LineBuffer()
        registered_worker: str | None = None
        session_id = uuid.uuid4().hex
        conn.settimeout(1.0)
        try:
            while not self._stop_event.is_set():
                try:
                    chunk = conn.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                for msg in buffer.feed(chunk):
                    registered_worker = self._handle_message(
                        conn, msg, registered_worker, session_id
                    )
        except (ProtocolError, ConnectionResetError, BrokenPipeError, OSError) as exc:
            if not self._stop_event.is_set():
                self._log(
                    f"Conexão encerrada ({registered_worker or addr}): {exc}"
                )
        finally:
            with self._state_lock:
                self._active_sockets.discard(conn)
            try:
                conn.close()
            except OSError:
                pass
            if registered_worker is not None and not self._stop_event.is_set():
                reclaimed = self.task_manager.reclaim_worker_tasks(
                    registered_worker, session_id=session_id
                )
                if reclaimed:
                    self._log(
                        f"Worker {registered_worker} (session={session_id[:8]}) desconectado; "
                        f"tarefas devolvidas para PENDING: {reclaimed}"
                    )

    def _reaper_loop(self) -> None:
        interval = max(0.1, min(1.0, self.lease_timeout_sec / 2.0))
        while not self._stop_event.wait(interval):
            reclaimed = self.task_manager.reclaim_expired_tasks()
            if reclaimed:
                self._log(f"Lease expirado para tarefas: {reclaimed}")

    def start(self) -> None:
        """Inicia o servidor TCP do Master e aceita conexões até `stop()`."""
        self._start_ts = time.perf_counter()
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(64)
        server_sock.settimeout(0.5)

        self._server_socket = server_sock
        self.port = server_sock.getsockname()[1]
        self._ready_event.set()

        self._log(
            f"Escutando em {self.host}:{self.port} | N={self.total_n} | "
            f"chunk_size={self.chunk_size} | lease={self.lease_timeout_sec}s | "
            f"tarefas={self.task_manager.get_summary()['total_tasks']}"
        )

        reaper = threading.Thread(target=self._reaper_loop, daemon=True)
        reaper.start()

        try:
            while not self._stop_event.is_set():
                try:
                    conn, addr = server_sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                with self._state_lock:
                    self._active_sockets.add(conn)
                handler = threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    daemon=True,
                )
                with self._state_lock:
                    self._client_threads.append(handler)
                handler.start()
        finally:
            self.stop()

    def wait_ready(self, timeout: float = 5.0) -> bool:
        """Aguarda o socket do servidor estar pronto para aceitar conexões."""
        return self._ready_event.wait(timeout=timeout)

    def wait_until_finished(self, timeout: float | None = None) -> bool:
        """Aguarda a conclusão de todas as tarefas do Grid."""
        return self._finished_event.wait(timeout=timeout)

    def stop(self) -> None:
        """Encerra o servidor TCP e fecha conexões ativas."""
        self._stop_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass
            self._server_socket = None

        with self._state_lock:
            sockets = list(self._active_sockets)
            self._active_sockets.clear()
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Master do Grid Computacional")
    parser.add_argument("--host", default="127.0.0.1", help="Endereço de bind")
    parser.add_argument("--port", type=int, default=5000, help="Porta TCP")
    parser.add_argument("--n", type=int, default=1_000_000, help="Limite superior N")
    parser.add_argument(
        "--chunk-size", type=int, default=50_000, help="Tamanho de cada subintervalo"
    )
    parser.add_argument(
        "--lease-timeout",
        type=float,
        default=10.0,
        help="Timeout de lease em segundos",
    )
    args = parser.parse_args()

    master = MasterNode(
        host=args.host,
        port=args.port,
        total_n=args.n,
        chunk_size=args.chunk_size,
        lease_timeout_sec=args.lease_timeout,
    )

    server_thread = threading.Thread(target=master.start, daemon=True)
    server_thread.start()
    master.wait_ready()

    try:
        master.wait_until_finished()
        # Aguarda brevemente para workers receberem NO_TASK(done=True)
        time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        master.stop()
        server_thread.join(timeout=2.0)


if __name__ == "__main__":
    main()
