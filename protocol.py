"""
Protocolo de comunicación compartido por cliente, dispatcher y workers.

Los sockets TCP son un flujo de bytes (stream) sin límites de mensaje.
Para saber dónde termina cada mensaje se usa "framing" con prefijo de longitud:

    +----------------+-----------------------------+
    | 4 bytes (!I)   | N bytes de JSON en UTF-8    |
    | largo N        | cuerpo del mensaje          |
    +----------------+-----------------------------+
"""
import json
import socket
import struct

HEADER = struct.Struct("!I")      # entero sin signo de 4 bytes, big-endian (orden de red)
MAX_MSG_BYTES = 10 * 1024 * 1024  # 10 MiB: protege la memoria ante mensajes corruptos o maliciosos


def send_msg(sock: socket.socket, obj: dict) -> None:
    """Serializa `obj` a JSON y lo envía con su prefijo de longitud."""
    body = json.dumps(obj).encode("utf-8")
    sock.sendall(HEADER.pack(len(body)) + body)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Lee exactamente n bytes. recv() puede devolver menos de lo pedido, por eso el bucle."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("el otro extremo cerró la conexión")
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock: socket.socket) -> dict:
    """Recibe un mensaje completo y lo deserializa."""
    (length,) = HEADER.unpack(_recv_exact(sock, HEADER.size))
    if length > MAX_MSG_BYTES:
        raise ValueError(f"mensaje demasiado grande: {length} bytes")
    return json.loads(_recv_exact(sock, length).decode("utf-8"))
