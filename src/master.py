# Nó coordenador (Master) do Grid Computacional sobre TCP com modelo Pull e Heartbeat.

from __future__ import annotations

import argparse
import json
import math
import socket
import threading
import time
import uuid
from pathlib import Path
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
    """Distribui tarefas do crivo ou pacotes de pixels por TCP."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5000,
        total_n: int = 1_000_000,
        chunk_size: int = 50_000,
        lease_timeout_sec: float = 10.0,
        verbose: bool = True,
        chunks_dir: str | Path | None = None,
        sobel_output: str | Path | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.total_n = total_n
        self.chunk_size = chunk_size
        self.lease_timeout_sec = lease_timeout_sec
        self.verbose = verbose
        self.pixel_chunk_files = (
            sorted(Path(chunks_dir).glob("chunk_*.json"))
            if chunks_dir is not None
            else None
        )
        self.sobel_output = (
            Path(sobel_output)
            if sobel_output is not None
            else Path(chunks_dir).parent / "sobel_output.json"
            if chunks_dir is not None
            else None
        )
        self.sobel_results: dict[int, list[float]] = {}

        if self.pixel_chunk_files is None:
            self.base_primes = generate_base_primes(math.isqrt(max(0, total_n)))
            task_total_n = total_n
            task_chunk_size = chunk_size
        else:
            self.base_primes = []
            task_total_n = len(self.pixel_chunk_files) + 1
            task_chunk_size = 1

        self.task_manager = TaskManager(
            total_n=task_total_n,
            chunk_size=task_chunk_size,
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
                task_payload: dict[str, Any] = {
                    "task_id": assignment.task_id,
                    "attempt_id": assignment.attempt_id,
                    "start": assignment.start,
                    "end": assignment.end,
                    "deadline_ts": assignment.deadline_ts,
                }
                if self.pixel_chunk_files is not None:
                    chunk_path = self.pixel_chunk_files[assignment.task_id]
                    with chunk_path.open("r", encoding="utf-8") as chunk_file:
                        pixel_chunk = json.load(chunk_file)
                    task_payload.update(
                        chunk_id=assignment.task_id,
                        start=pixel_chunk["row_start"],
                        end=pixel_chunk["row_end"] - 1,
                        image_width=pixel_chunk["image_width"],
                        image_height=pixel_chunk["image_height"],
                        channels=pixel_chunk["channels"],
                        row_start=pixel_chunk["row_start"],
                        row_end=pixel_chunk["row_end"],
                        input_row_start=pixel_chunk["input_row_start"],
                        input_row_end=pixel_chunk["input_row_end"],
                        pixels=pixel_chunk["pixels"],
                    )
                    self._log(
                        f"Chunk {assignment.task_id} linhas "
                        f"[{pixel_chunk['row_start']}, {pixel_chunk['row_end']}) "
                        f"enviado para {worker_id}"
                    )
                else:
                    self._log(
                        f"Atribuída tarefa {assignment.task_id} (attempt={assignment.attempt_id}) "
                        f"[{assignment.start}, {assignment.end}] -> {worker_id}"
                    )
                self._send(conn, MessageType.TASK_ASSIGN, **task_payload)
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
            if self.pixel_chunk_files is not None:
                task_id = msg["task_id"]
                if "sobel_pixels" not in msg:
                    raise ProtocolError("SUBMIT_RESULT de imagem precisa conter sobel_pixels.")
                if task_id >= len(self.pixel_chunk_files):
                    raise ProtocolError("task_id inválido para resultado Sobel.")
                with self.pixel_chunk_files[task_id].open("r", encoding="utf-8") as source:
                    chunk = json.load(source)
                expected_rows = (chunk["row_end"] - chunk["row_start"]) * chunk["image_width"]
                if (
                    msg.get("row_start") != chunk["row_start"]
                    or msg.get("row_end") != chunk["row_end"]
                    or len(msg["sobel_pixels"]) != expected_rows
                ):
                    raise ProtocolError("Dimensões do resultado Sobel não correspondem ao chunk.")

            outcome = self.task_manager.submit_result(
                worker_id=msg["worker_id"],
                task_id=msg["task_id"],
                attempt_id=msg["attempt_id"],
                partial_sum=msg.get("partial_sum", 0),
                primes_count=msg.get("primes_count", 0),
                elapsed_ms=msg["elapsed_ms"],
            )
            if outcome.accepted:
                if self.pixel_chunk_files is not None:
                    with self._state_lock:
                        self.sobel_results[msg["task_id"]] = msg["sobel_pixels"]
                summary = self.task_manager.get_summary()
                if self.pixel_chunk_files is not None:
                    self._log(f"Chunk Sobel {outcome.task_id} recebido de {msg['worker_id']}")
                else:
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
                    if self.pixel_chunk_files is not None:
                        self._write_sobel_output()
                    elapsed_ms = (
                        (time.perf_counter() - self._start_ts) * 1000.0
                        if self._start_ts is not None
                        else 0.0
                    )
                    if self.pixel_chunk_files is not None:
                        self._log(f"SOBEL CONCLUÍDO: saída={self.sobel_output} | elapsed_ms={elapsed_ms:.2f}")
                    else:
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

    def _write_sobel_output(self) -> None:
        """Junta as linhas dos chunks e grava uma imagem Sobel em JSON."""
        if self.pixel_chunk_files is None or self.sobel_output is None:
            return
        pixels: list[float] = []
        first_chunk = json.loads(self.pixel_chunk_files[0].read_text(encoding="utf-8"))
        for task_id in range(len(self.pixel_chunk_files)):
            pixels.extend(self.sobel_results[task_id])
        output = {
            "width": first_chunk["image_width"],
            "height": first_chunk["image_height"],
            "channels": ["sobel_nir"],
            "layout": "row_major_one_pixel_per_row",
            "pixels": pixels,
        }
        self.sobel_output.parent.mkdir(parents=True, exist_ok=True)
        self.sobel_output.write_text(json.dumps(output, separators=(",", ":")), encoding="utf-8")

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

        workload = (
            f"pixel_chunks={len(self.pixel_chunk_files)}"
            if self.pixel_chunk_files is not None
            else f"N={self.total_n} | chunk_size={self.chunk_size}"
        )
        self._log(
            f"Escutando em {self.host}:{self.port} | {workload} | "
            f"lease={self.lease_timeout_sec}s | "
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
    parser.add_argument(
        "--chunks-dir",
        type=Path,
        default=None,
        help="Diretório com chunk_*.json para distribuir pixels em vez de intervalos do crivo",
    )
    parser.add_argument("--sobel-output", type=Path, default=None, help="JSON final da imagem Sobel")
    args = parser.parse_args()

    master = MasterNode(
        host=args.host,
        port=args.port,
        total_n=args.n,
        chunk_size=args.chunk_size,
        lease_timeout_sec=args.lease_timeout,
        chunks_dir=args.chunks_dir,
        sobel_output=args.sobel_output,
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
