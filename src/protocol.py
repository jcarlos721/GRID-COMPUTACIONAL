# Protocolo de comunicação JSON-over-TCP (por linha) e tratamento de buffer parcial.

from __future__ import annotations

import json
from enum import Enum
from typing import Any


class MessageType(str, Enum):
    """Tipos de mensagens suportados entre Master e Worker."""

    REGISTER = "REGISTER"
    REGISTERED = "REGISTERED"
    GET_TASK = "GET_TASK"
    TASK_ASSIGN = "TASK_ASSIGN"
    HEARTBEAT = "HEARTBEAT"
    SUBMIT_RESULT = "SUBMIT_RESULT"
    NO_TASK = "NO_TASK"
    ACK = "ACK"
    REJECT = "REJECT"


class ProtocolError(ValueError):
    """Erro lançado quando uma mensagem viola o formato ou contrato do protocolo."""


_REQUIRED_FIELDS: dict[MessageType, dict[str, tuple[type, ...]]] = {
    MessageType.REGISTER: {
        "worker_id": (str,),
    },
    MessageType.REGISTERED: {
        "worker_id": (str,),
        "base_primes": (list,),
        "lease_timeout_sec": (int, float),
    },
    MessageType.GET_TASK: {
        "worker_id": (str,),
    },
    MessageType.TASK_ASSIGN: {
        "task_id": (int,),
        "attempt_id": (int,),
        "start": (int,),
        "end": (int,),
        "deadline_ts": (int, float),
    },
    MessageType.HEARTBEAT: {
        "worker_id": (str,),
        "task_id": (int,),
        "attempt_id": (int,),
    },
    MessageType.SUBMIT_RESULT: {
        "worker_id": (str,),
        "task_id": (int,),
        "attempt_id": (int,),
        "elapsed_ms": (int, float),
    },
    MessageType.NO_TASK: {},
    MessageType.ACK: {
        "task_id": (int,),
        "attempt_id": (int,),
        "accepted": (bool,),
    },
    MessageType.REJECT: {
        "task_id": (int,),
        "attempt_id": (int,),
        "accepted": (bool,),
        "reason": (str,),
    },
}


def _is_valid_type(value: Any, expected: tuple[type, ...]) -> bool:
    """Verifica tipo evitando que bool seja aceito como int."""
    if isinstance(value, bool):
        return bool in expected
    return isinstance(value, expected)


def validate_message(message: dict[str, Any]) -> dict[str, Any]:
    """Valida a estrutura básica e os campos obrigatórios de uma mensagem."""
    if not isinstance(message, dict):
        raise ProtocolError("Mensagem deve ser um objeto JSON.")

    raw_type = message.get("type")
    if not isinstance(raw_type, str):
        raise ProtocolError("Campo 'type' ausente ou inválido.")

    try:
        msg_type = MessageType(raw_type)
    except ValueError as exc:
        raise ProtocolError(f"Tipo de mensagem desconhecido: {raw_type!r}") from exc

    schema = _REQUIRED_FIELDS[msg_type]
    for field_name, expected_types in schema.items():
        if field_name not in message:
            raise ProtocolError(
                f"Campo obrigatório '{field_name}' ausente em {msg_type.value}."
            )
        if not _is_valid_type(message[field_name], expected_types):
            raise ProtocolError(
                f"Campo '{field_name}' com tipo inválido em {msg_type.value}."
            )

    if msg_type == MessageType.REGISTERED:
        if not all(_is_valid_type(p, (int,)) for p in message["base_primes"]):
            raise ProtocolError("'base_primes' deve conter apenas inteiros.")
    elif msg_type == MessageType.SUBMIT_RESULT:
        if "sobel_pixels" in message:
            if not isinstance(message["sobel_pixels"], list):
                raise ProtocolError("'sobel_pixels' deve ser uma lista.")
            for field in ("row_start", "row_end"):
                if not _is_valid_type(message.get(field), (int,)):
                    raise ProtocolError(f"'{field}' deve ser inteiro no resultado Sobel.")
        else:
            for field in ("partial_sum", "primes_count"):
                if not _is_valid_type(message.get(field), (int,)):
                    raise ProtocolError(f"Campo obrigatório '{field}' ausente ou inválido.")
            if message["partial_sum"] < 0:
                raise ProtocolError("'partial_sum' não pode ser negativo.")
            if message["primes_count"] < 0:
                raise ProtocolError("'primes_count' não pode ser negativo.")
    elif msg_type == MessageType.ACK and message["accepted"] is not True:
        raise ProtocolError("Mensagem ACK deve ter 'accepted=True'.")
    elif msg_type == MessageType.REJECT and message["accepted"] is not False:
        raise ProtocolError("Mensagem REJECT deve ter 'accepted=False'.")

    return message


def encode_message(msg_type: MessageType | str, **payload: Any) -> bytes:
    """Serializa uma mensagem do protocolo para bytes JSON terminados em '\\n'."""
    resolved_type = msg_type.value if isinstance(msg_type, MessageType) else msg_type
    message = {"type": resolved_type, **payload}
    validate_message(message)
    serialized = json.dumps(message, separators=(",", ":")) + "\n"
    return serialized.encode("utf-8")


def decode_message(raw_line: str | bytes) -> dict[str, Any]:
    """Desserializa uma linha JSON (str ou bytes) e valida os campos do protocolo."""
    if isinstance(raw_line, bytes):
        try:
            text = raw_line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("Payload não é UTF-8 válido.") from exc
    else:
        text = raw_line

    stripped = text.strip()
    if not stripped:
        raise ProtocolError("Linha de mensagem vazia.")

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"JSON inválido: {exc}") from exc

    return validate_message(parsed)


class LineBuffer:
    """Buffer incremental para framing de mensagens JSON delimitadas por '\\n' em TCP."""

    def __init__(self, max_buffer_bytes: int = 16 * 1024 * 1024) -> None:
        self._buffer = bytearray()
        self._max_buffer_bytes = max_buffer_bytes

    def feed(self, chunk: bytes) -> list[dict[str, Any]]:
        """Adiciona bytes recebidos do socket e retorna mensagens completas decodificadas."""
        if not chunk:
            return []

        if len(self._buffer) + len(chunk) > self._max_buffer_bytes:
            raise ProtocolError("Tamanho máximo do buffer de entrada excedido.")

        self._buffer.extend(chunk)
        messages: list[dict[str, Any]] = []

        while True:
            newline_idx = self._buffer.find(b"\n")
            if newline_idx == -1:
                break

            raw_line = bytes(self._buffer[:newline_idx])
            del self._buffer[: newline_idx + 1]

            if not raw_line.strip():
                continue

            messages.append(decode_message(raw_line))

        return messages

    def has_pending_data(self) -> bool:
        """Indica se há bytes parciais aguardando o próximo '\\n'."""
        return len(self._buffer) > 0

    def clear(self) -> None:
        """Limpa o buffer interno."""
        self._buffer.clear()
