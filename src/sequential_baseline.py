# Execução sequencial de referência (baseline) para validação e comparação de desempenho.

from __future__ import annotations

import math
import sys
import time
from dataclasses import dataclass

from src.sieve import generate_base_primes, sum_primes_in_range


@dataclass(frozen=True)
class BaselineResult:
    """Resultado consolidado da execução sequencial até N."""

    n: int
    prime_sum: int
    primes_count: int
    elapsed_ms: float


def run_sequential_baseline(
    n: int,
    segment_size: int = 1_000_000,
) -> BaselineResult:
    """Calcula a soma e contagem de primos até `n` sequencialmente medindo o tempo."""
    if segment_size <= 0:
        raise ValueError("segment_size deve ser maior que zero.")

    start_ts = time.perf_counter()
    if n < 2:
        elapsed_ms = (time.perf_counter() - start_ts) * 1000.0
        return BaselineResult(n=n, prime_sum=0, primes_count=0, elapsed_ms=elapsed_ms)

    base_primes = generate_base_primes(math.isqrt(n))
    total_sum = 0
    total_count = 0

    current = 2
    while current <= n:
        end = min(current + segment_size - 1, n)
        part_sum, part_count = sum_primes_in_range(current, end, base_primes=base_primes)
        total_sum += part_sum
        total_count += part_count
        current = end + 1

    elapsed_ms = (time.perf_counter() - start_ts) * 1000.0
    return BaselineResult(
        n=n,
        prime_sum=total_sum,
        primes_count=total_count,
        elapsed_ms=elapsed_ms,
    )


if __name__ == "__main__":
    target_n = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    result = run_sequential_baseline(target_n)
    print(
        f"N={result.n} | primes={result.primes_count} | "
        f"sum={result.prime_sum} | elapsed_ms={result.elapsed_ms:.2f}"
    )
