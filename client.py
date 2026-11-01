"""
Cliente: envía tareas al dispatcher por socket y muestra los resultados.

Ejemplos:
    python client.py --task fibonacci --payload 90
    python client.py --task upper_text --payload '"hola mundo"'
    python client.py --demo --concurrency 6
    python client.py --status
"""
import argparse
import json
import socket
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from protocol import recv_msg, send_msg


def request(host, port, msg, timeout=60.0):
    """Abre una conexión, envía `msg` y devuelve la respuesta."""
    with socket.create_connection((host, port), timeout=timeout) as s:
        send_msg(s, msg)
        return recv_msg(s)


def send_task(host, port, task, payload, timeout=60.0):
    return request(host, port, {"type": "task", "id": uuid.uuid4().hex[:8],
                                "task": task, "payload": payload}, timeout)


DEMO_TASKS = (
    [("sleep", 1.0)] * 6
    + [("fibonacci", 5000), ("prime_count", 1_000_000), ("sum_list", [1, 2, 3.5, 10]),
       ("upper_text", "sistemas distribuidos"), ("word_count", "uno dos tres cuatro"),
       ("tarea_inexistente", None)]  # a propósito: muestra el manejo de errores
)


def run_demo(host, port, concurrency):
    t0 = time.perf_counter()

    def job(spec):
        try:
            return spec, send_task(host, port, *spec)
        except OSError as exc:
            return spec, {"ok": False, "error": f"conexión: {exc}"}

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        results = list(pool.map(job, DEMO_TASKS))

    print(f"{'id':<9}{'tarea':<18}{'worker':<12}{'ms':>9}  resultado")
    for (name, _), r in results:
        detail = str(r.get("result", r.get("error")))
        print(f"{r.get('id', '-'):<9}{name:<18}{str(r.get('worker', '-')):<12}"
              f"{r.get('total_ms', 0):>9}  {detail[:60]}")
    print(f"\n{len(results)} tareas en {time.perf_counter() - t0:.2f} s "
          f"(concurrencia del cliente = {concurrency})")


def main():
    p = argparse.ArgumentParser(description="Cliente del sistema distribuido")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--task")
    p.add_argument("--payload", default="null", help="JSON; si no es válido se envía como texto")
    p.add_argument("--demo", action="store_true")
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--status", action="store_true")
    args = p.parse_args()

    try:
        if args.status:
            print(json.dumps(request(args.host, args.port, {"type": "status"}), indent=2))
        elif args.demo:
            run_demo(args.host, args.port, args.concurrency)
        elif args.task:
            try:
                payload = json.loads(args.payload)
            except json.JSONDecodeError:
                payload = args.payload
            print(json.dumps(send_task(args.host, args.port, args.task, payload), indent=2))
        else:
            p.error("indicar --task, --demo o --status")
    except OSError as exc:
        raise SystemExit(f"No se pudo conectar a {args.host}:{args.port}: {exc}")


if __name__ == "__main__":
    main()
