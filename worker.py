"""
Servidor worker: ejecuta tareas recibidas por socket usando un pool de hilos.

Modelo de concurrencia:
  - El hilo principal solo hace accept().
  - Cada conexión aceptada se entrega a un ThreadPoolExecutor de tamaño fijo.
  - Si todos los hilos están ocupados, las conexiones nuevas esperan en la cola
    interna del executor (y en el backlog de TCP): hay contrapresión natural y
    nunca se crean hilos sin límite.

Uso:
    python worker.py --port 9001 --pool 4 --name worker-1
"""
import argparse
import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from protocol import recv_msg, send_msg
from tasks import run_task

log = logging.getLogger("worker")


class Worker:
    def __init__(self, host: str, port: int, pool_size: int, name: str):
        self.host, self.port, self.name = host, port, name
        self.pool = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix=f"{name}-t")
        self._stop = threading.Event()

    def serve_forever(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(128)
            srv.settimeout(1.0)  # permite revisar _stop periódicamente
            log.info("%s escuchando en %s:%d (pool=%d)", self.name, self.host, self.port,
                     self.pool._max_workers)
            while not self._stop.is_set():
                try:
                    conn, addr = srv.accept()
                except socket.timeout:
                    continue
                self.pool.submit(self._handle, conn, addr)

    def stop(self) -> None:
        self._stop.set()
        self.pool.shutdown(wait=False, cancel_futures=True)

    def _handle(self, conn: socket.socket, addr) -> None:
        """Atiende una solicitud: recibe, ejecuta, responde, cierra."""
        with conn:
            conn.settimeout(30)
            try:
                msg = recv_msg(conn)
                send_msg(conn, self._execute(msg))
            except (ConnectionError, OSError, ValueError) as exc:
                log.warning("error con %s: %s", addr, exc)

    def _execute(self, msg: dict) -> dict:
        t0 = time.perf_counter()
        base = {"worker": self.name, "thread": threading.current_thread().name}
        try:
            result = run_task(msg.get("task"), msg.get("payload"))
            return {**base, "ok": True, "result": result,
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2)}
        except Exception as exc:  # un error de tarea no debe tumbar al worker
            return {**base, "ok": False, "error": f"{type(exc).__name__}: {exc}",
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2)}


def main() -> None:
    p = argparse.ArgumentParser(description="Servidor worker con pool de hilos")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--pool", type=int, default=4, help="hilos del pool")
    p.add_argument("--name", default=None)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    worker = Worker(args.host, args.port, args.pool, args.name or f"worker-{args.port}")
    try:
        worker.serve_forever()
    except KeyboardInterrupt:
        log.info("cerrando...")
    finally:
        worker.stop()


if __name__ == "__main__":
    main()
