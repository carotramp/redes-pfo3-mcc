"""
Bus de eventos entre servidores (RabbitMQ).

El dispatcher publica un evento por cada tarea terminada en la cola
"task_events". Otros servicios (auditoría, métricas, notificaciones) pueden
consumirla sin acoplarse al dispatcher.

Es opcional y degrada con elegancia: si RABBITMQ_URL no está definida, si
`pika` no está instalado o si el broker cae, el sistema sigue funcionando y
solo se registra una advertencia. Un fallo del bus nunca debe afectar la
respuesta al cliente.
"""
import json
import logging
import os
import threading

log = logging.getLogger("events")
QUEUE = "task_events"


class EventBus:
    def __init__(self, url=None):
        self.url = url or os.environ.get("RABBITMQ_URL")  # p. ej. amqp://guest:guest@localhost:5672/%2F
        self._lock = threading.Lock()  # BlockingConnection de pika no es thread-safe
        self._conn = self._ch = None
        if self.url:
            log.info("RabbitMQ habilitado")
        else:
            log.info("RabbitMQ deshabilitado (definir RABBITMQ_URL para activarlo)")

    def _connect(self):
        import pika
        self._conn = pika.BlockingConnection(pika.URLParameters(self.url))
        self._ch = self._conn.channel()
        self._ch.queue_declare(queue=QUEUE, durable=True)

    def publish(self, event: dict) -> bool:
        if not self.url:
            return False
        with self._lock:
            try:
                if self._conn is None or self._conn.is_closed:
                    self._connect()
                import pika
                self._ch.basic_publish(
                    exchange="", routing_key=QUEUE, body=json.dumps(event).encode(),
                    properties=pika.BasicProperties(delivery_mode=2),  # mensaje persistente
                )
                return True
            except Exception as exc:
                log.warning("no se pudo publicar evento: %s", exc)
                self._conn = self._ch = None
                return False
