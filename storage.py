"""
Persistencia de resultados.

Aquí se usa SQLite para que el TP se ejecute sin instalar nada. En la
arquitectura objetivo este módulo se reemplaza por PostgreSQL (metadatos y
resultados) y S3 (resultados voluminosos / archivos); la interfaz `save()` es
la misma, por lo que el cambio no afecta al resto del código.
"""
import json
import sqlite3
import threading
import time


class ResultStore:
    def __init__(self, path: str = "results.db"):
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=10)
        self._lock = threading.Lock()  # una conexión compartida entre hilos exige serializar el acceso
        with self._lock, self._conn:
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS tasks (
                       id TEXT, client TEXT, task TEXT, payload TEXT,
                       ok INTEGER, result TEXT, worker TEXT,
                       elapsed_ms REAL, created_at REAL)"""
            )

    def save(self, rec: dict) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?)",
                (rec.get("id"), rec.get("client"), rec.get("task"),
                 json.dumps(rec.get("payload"))[:2000], int(bool(rec.get("ok"))),
                 json.dumps(rec.get("result", rec.get("error")))[:10000],
                 rec.get("worker"), rec.get("elapsed_ms"), time.time()),
            )

    def close(self) -> None:
        self._conn.close()
