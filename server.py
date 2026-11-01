"""
Servidor dispatcher (gateway): recibe tareas de los clientes por socket y las
distribuye entre los workers.

Responsabilidades:
  1. Aceptar conexiones de clientes (un hilo del pool de conexiones por cliente).
  2. Elegir un worker con una estrategia de balanceo (round_robin o least_connections).
  3. Reenviar la tarea al worker por socket y devolver la respuesta al cliente.
  4. Tolerar fallos: si un worker no responde se lo marca caído por un período
     (cooldown) y se reintenta con otro (failover).
  5. Persistir resultados y publicar eventos (RabbitMQ, opcional).

Uso:
    python server.py --port 9000 --workers 127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003
"""
import argparse
import logging
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from events import EventBus
from protocol import recv_msg, send_msg
from storage import ResultStore

log = logging.getLogger("dispatcher")


class WorkerNode:
    """Estado de un worker visto desde el dispatcher."""

    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self.active = 0          # tareas en vuelo hacia este worker
        self.served = 0          # tareas completadas
        self.down_until = 0.0    # instante (monotonic) hasta el cual se considera caído

    @property
    def addr(self):
        return (self.host, self.port)

    def __repr__(self):
        return f"{self.host}:{self.port}"


class Balancer:
    """Selección de worker. Todas las operaciones están protegidas por un lock."""

    def __init__(self, nodes, strategy="least_connections", cooldown=5.0):
        self.nodes, self.strategy, self.cooldown = nodes, strategy, cooldown
        self._lock = threading.Lock()
        self._rr = -1

    def acquire(self, exclude=()):
        """Elige un worker disponible y reserva un slot (active += 1). None si no hay."""
        with self._lock:
            now = time.monotonic()
            cands = [n for n in self.nodes if n.down_until <= now and n not in exclude]
            if not cands:
                return None
            if self.strategy == "round_robin":
                self._rr += 1
                node = cands[self._rr % len(cands)]
            else:  # least_connections: el menos cargado; desempata por menos servidas
                node = min(cands, key=lambda n: (n.active, n.served))
            node.active += 1
            return node

    def release(self, node, failed=False):
        with self._lock:
            node.active -= 1
            if failed:
                node.down_until = time.monotonic() + self.cooldown
            else:
                node.served += 1

    def snapshot(self):
        with self._lock:
            now = time.monotonic()
            return [{"worker": repr(n), "active": n.active, "served": n.served,
                     "up": n.down_until <= now} for n in self.nodes]


class Dispatcher:
    def __init__(self, host, port, nodes, strategy, max_clients, task_timeout, store, bus):
        self.host, self.port = host, port
        self.balancer = Balancer(nodes, strategy)
        self.task_timeout = task_timeout
        self.store, self.bus = store, bus
        self.clients = ThreadPoolExecutor(max_workers=max_clients, thread_name_prefix="client")
        self._stop = threading.Event()

    # ---------- ciclo de aceptación ----------
    def serve_forever(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(128)
            srv.settimeout(1.0)
            log.info("dispatcher en %s:%d, estrategia=%s, workers=%s",
                     self.host, self.port, self.balancer.strategy, self.balancer.nodes)
            while not self._stop.is_set():
                try:
                    conn, addr = srv.accept()
                except socket.timeout:
                    continue
                self.clients.submit(self._handle_client, conn, addr)

    def stop(self):
        self._stop.set()
        self.clients.shutdown(wait=False, cancel_futures=True)
        self.store.close()

    # ---------- atención de clientes ----------
    def _handle_client(self, conn, addr):
        """Una conexión puede enviar varias solicitudes secuenciales."""
        with conn:
            while True:
                try:
                    msg = recv_msg(conn)
                except (ConnectionError, OSError, ValueError):
                    break  # el cliente cerró o envió basura
                reply = self._process(msg, f"{addr[0]}:{addr[1]}")
                try:
                    send_msg(conn, reply)
                except OSError:
                    break

    def _process(self, msg, client):
        if not isinstance(msg, dict):
            return {"ok": False, "error": "mensaje inválido"}
        kind = msg.get("type", "task")
        if kind == "status":
            return {"ok": True, "workers": self.balancer.snapshot()}
        if kind != "task":
            return {"ok": False, "error": f"tipo desconocido: {kind!r}"}

        task_id = msg.get("id") or uuid.uuid4().hex[:8]
        t0 = time.perf_counter()
        resp = self.dispatch({"task": msg.get("task"), "payload": msg.get("payload")})
        resp["id"] = task_id
        resp["total_ms"] = round((time.perf_counter() - t0) * 1000, 2)

        record = {"id": task_id, "client": client, "task": msg.get("task"),
                  "payload": msg.get("payload"), **{k: resp.get(k) for k in
                  ("ok", "result", "error", "worker", "elapsed_ms")}}
        try:
            self.store.save(record)
        except Exception as exc:
            log.warning("no se pudo guardar el resultado: %s", exc)
        self.bus.publish({k: v for k, v in record.items() if k != "payload"})
        return resp

    # ---------- distribución a workers ----------
    def dispatch(self, req):
        """Envía `req` a un worker; si falla la conexión o expira, prueba con otro."""
        tried = set()
        while True:
            node = self.balancer.acquire(exclude=tried)
            if node is None:
                return {"ok": False, "error": "no hay workers disponibles"}
            tried.add(node)
            try:
                with socket.create_connection(node.addr, timeout=2.0) as s:
                    s.settimeout(self.task_timeout)
                    send_msg(s, req)
                    resp = recv_msg(s)
                self.balancer.release(node)
                return resp
            except (OSError, ConnectionError, ValueError) as exc:
                self.balancer.release(node, failed=True)
                log.warning("worker %s falló (%s); failover", node, exc)


def parse_nodes(items):
    nodes = []
    for item in items:
        host, _, port = item.rpartition(":")
        nodes.append(WorkerNode(host or "127.0.0.1", int(port)))
    return nodes


def main():
    p = argparse.ArgumentParser(description="Servidor dispatcher cliente-servidor")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--workers", nargs="+", required=True, help="lista host:puerto")
    p.add_argument("--strategy", choices=["round_robin", "least_connections"],
                   default="least_connections")
    p.add_argument("--max-clients", type=int, default=100)
    p.add_argument("--task-timeout", type=float, default=30.0)
    p.add_argument("--db", default="results.db")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    srv = Dispatcher(args.host, args.port, parse_nodes(args.workers), args.strategy,
                     args.max_clients, args.task_timeout, ResultStore(args.db), EventBus())
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log.info("cerrando...")
    finally:
        srv.stop()


if __name__ == "__main__":
    main()
