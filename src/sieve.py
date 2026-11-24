# Crivo de Eratóstenes clássico (primos base) e crivo segmentado para intervalos [start, end].

from __future__ import annotations

import math


def generate_base_primes(limit: int) -> list[int]:
    """Gera todos os números primos até `limit` (inclusive) usando Crivo de Eratóstenes."""
    if limit < 2:
        return []

    is_prime = bytearray(b"\x01") * (limit + 1)
    is_prime[0] = 0
    is_prime[1] = 0

    upper = math.isqrt(limit)
    for p in range(2, upper + 1):
        if is_prime[p]:
            start_sq = p * p
            count = ((limit - start_sq) // p) + 1
            is_prime[start_sq : limit + 1 : p] = b"\x00" * count

    return [num for num, flag in enumerate(is_prime) if flag]


def sum_primes_in_range(
    start: int,
    end: int,
    base_primes: list[int] | None = None,
) -> tuple[int, int]:
    """Retorna `(partial_sum, primes_count)` para os primos no intervalo fechado `[start, end]`."""
    if end < 2 or start > end:
        return (0, 0)

    effective_start = max(2, start)
    if effective_start > end:
        return (0, 0)

    if base_primes is None:
        base_primes = generate_base_primes(math.isqrt(end))

    segment_len = end - effective_start + 1
    is_prime = bytearray(b"\x01") * segment_len

    limit_sqrt = math.isqrt(end)
    for p in base_primes:
        if p > limit_sqrt:
            break
        if p < 2:
            continue

        first_multiple = ((effective_start + p - 1) // p) * p
        if first_multiple < p * p:
            first_multiple = p * p
        if first_multiple > end:
            continue

        offset = first_multiple - effective_start
        count = ((end - first_multiple) // p) + 1
        is_prime[offset::p] = b"\x00" * count

    partial_sum = 0
    primes_count = 0
    for idx, flag in enumerate(is_prime):
        if flag:
            partial_sum += effective_start + idx
            primes_count += 1

    return (partial_sum, primes_count)
