"""
Catálogo de tareas que un worker sabe ejecutar.

Se usa un registro explícito (lista blanca) en lugar de eval/exec: un worker
jamás ejecuta código arbitrario recibido por red.
"""
import time

MAX_FIB_N = 10_000          # fib(10000) ~ 2090 dígitos (por debajo del límite de str() de Python)
MAX_PRIME_N = 2_000_000
MAX_SLEEP_S = 10.0
MAX_LIST_LEN = 100_000


def fibonacci(n) -> str:
    """Fibonacci iterativo (CPU). Devuelve str porque el entero puede ser enorme."""
    n = int(n)
    if not 0 <= n <= MAX_FIB_N:
        raise ValueError(f"n debe estar entre 0 y {MAX_FIB_N}")
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return str(a)


def prime_count(n) -> int:
    """Cantidad de primos <= n con la criba de Eratóstenes (CPU)."""
    n = int(n)
    if not 0 <= n <= MAX_PRIME_N:
        raise ValueError(f"n debe estar entre 0 y {MAX_PRIME_N}")
    if n < 2:
        return 0
    sieve = bytearray([1]) * (n + 1)
    sieve[0] = sieve[1] = 0
    for i in range(2, int(n ** 0.5) + 1):
        if sieve[i]:
            sieve[i * i::i] = bytearray(len(range(i * i, n + 1, i)))
    return sum(sieve)


def sum_list(numbers) -> float:
    """Suma una lista de números."""
    if not isinstance(numbers, list) or len(numbers) > MAX_LIST_LEN:
        raise ValueError(f"se espera una lista de hasta {MAX_LIST_LEN} números")
    return float(sum(float(x) for x in numbers))


def upper_text(text) -> str:
    """Convierte a mayúsculas."""
    return str(text).upper()


def word_count(text) -> int:
    """Cuenta palabras."""
    return len(str(text).split())


def sleep_task(seconds) -> float:
    """Simula una tarea limitada por E/S (espera). Útil para ver la concurrencia."""
    seconds = float(seconds)
    if not 0 <= seconds <= MAX_SLEEP_S:
        raise ValueError(f"seconds debe estar entre 0 y {MAX_SLEEP_S}")
    time.sleep(seconds)
    return seconds


REGISTRY = {
    "fibonacci": fibonacci,
    "prime_count": prime_count,
    "sum_list": sum_list,
    "upper_text": upper_text,
    "word_count": word_count,
    "sleep": sleep_task,
}


def run_task(name, payload):
    """Ejecuta la tarea `name` con `payload`. Lanza ValueError si la tarea no existe."""
    func = REGISTRY.get(name)
    if func is None:
        raise ValueError(f"tarea desconocida: {name!r}. Disponibles: {sorted(REGISTRY)}")
    return func(payload)
