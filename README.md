# PFO 3 - Rediseño como Sistema Distribuido (Cliente-Servidor)

Sistema de procesamiento de tareas distribuido con sockets TCP en Python: clientes, balanceador de carga,
servidores dispatcher, workers con pool de hilos, cola de mensajes (RabbitMQ) y almacenamiento distribuido.

## 1. Diagrama

Versión gráfica: [`diagrama.svg`](diagrama.svg). Versión Mermaid (GitHub la renderiza):

```mermaid
flowchart TB
    subgraph C[Clientes]
        M[Cliente movil]
        W[Cliente web]
    end
    LB[Balanceador de carga<br/>Nginx / HAProxy - L4 TCP]
    subgraph D[Servidores dispatcher - server.py]
        D1[Dispatcher 1]
        D2[Dispatcher 2]
    end
    subgraph WK[Servidores workers - worker.py]
        W1[Worker 1<br/>pool de hilos]
        W2[Worker 2<br/>pool de hilos]
        W3[Worker 3<br/>pool de hilos]
    end
    MQ[(RabbitMQ<br/>cola task_events)]
    subgraph ST[Almacenamiento distribuido]
        PG[(PostgreSQL)]
        S3[(Amazon S3)]
    end
    M --> LB
    W --> LB
    LB --> D1
    LB --> D2
    D1 --> W1 & W2 & W3
    D2 --> W1 & W2 & W3
    D1 -.eventos.-> MQ
    D2 -.eventos.-> MQ
    W1 -.eventos.-> MQ
    W2 -.eventos.-> MQ
    W3 -.eventos.-> MQ
    W1 --> PG
    W2 --> PG
    W3 --> S3
```

## 2. Explicación paso a paso del diseño

**Paso 1 - Problema del sistema monolítico.** Un único proceso tiene un techo de capacidad (CPU, memoria) y es un
punto único de falla. Al distribuirlo se gana escalabilidad horizontal y tolerancia a fallos, a cambio de
complejidad de red (latencia, fallos parciales, mensajes que se pierden o se duplican).

**Paso 2 - Capas y responsabilidades.**

| Capa | Componente | Responsabilidad |
|---|---|---|
| 1 | Clientes móvil / web | Generan tareas y esperan resultados por socket |
| 2 | Balanceador (Nginx/HAProxy) | Reparte conexiones entre dispatchers a nivel TCP (L4), con health checks |
| 3 | Dispatcher (`server.py`) | Recibe tareas, elige worker, reintenta ante fallos, persiste resultados |
| 4 | Workers (`worker.py`) | Ejecutan la tarea con un pool de hilos de tamaño fijo |
| - | RabbitMQ | Desacopla servidores: el dispatcher publica eventos `task_events` para auditoría/métricas |
| 5 | PostgreSQL + S3 | PostgreSQL: metadatos/resultados; S3: archivos y resultados voluminosos |

**Paso 3 - Protocolo sobre sockets (`protocol.py`).** TCP es un flujo de bytes sin límites de mensaje. Se usa
*framing* con prefijo de longitud: 4 bytes (`!I`, big-endian) con el largo, seguidos de JSON UTF-8. `recv()` puede
devolver menos bytes de los pedidos, por eso `_recv_exact` itera hasta completar. Se limita el tamaño máximo (10 MiB).

**Paso 4 - Worker con pool de hilos (`worker.py`).** El hilo principal solo ejecuta `accept()` y entrega cada
conexión a un `ThreadPoolExecutor` de tamaño fijo. Si el pool está saturado, las conexiones esperan en cola: hay
contrapresión y nunca se crean hilos sin límite. Los errores de una tarea se capturan y se devuelven como
respuesta `ok: false`; no tumban al worker.

**Paso 5 - Tareas seguras (`tasks.py`).** Lista blanca de funciones con límites de entrada. Nunca se usa
`eval`/`exec` sobre datos recibidos por red.

**Paso 6 - Dispatcher y balanceo (`server.py`).** Dos estrategias: `round_robin` y `least_connections`
(por defecto; elige el worker con menos tareas en vuelo). El estado compartido entre hilos se protege con un
`Lock`. **Failover:** si la conexión a un worker falla o expira, se lo marca caído `cooldown` segundos y la
tarea se reintenta con otro worker; si ninguno responde, el cliente recibe `no hay workers disponibles`.

**Paso 7 - Cliente (`client.py`).** Envía una tarea (`--task`), un lote concurrente (`--demo`) o consulta el
estado del cluster (`--status`).

**Paso 8 - Mensajería y persistencia (`events.py`, `storage.py`).** El dispatcher guarda cada resultado
(SQLite aquí, para correr sin instalar nada; en producción se reemplaza por PostgreSQL/S3 manteniendo `save()`)
y publica un evento en RabbitMQ si `RABBITMQ_URL` está definida. Si el broker no está disponible, el sistema
sigue funcionando: un fallo del bus no debe afectar la respuesta al cliente.

## 3. Cómo ejecutarlo

Requisitos: Python 3.8+ (el núcleo no necesita paquetes externos).

```bash
# Terminal 1-3: workers (cada uno con pool de 4 hilos)
python worker.py --port 9001 --pool 4 --name worker-1
python worker.py --port 9002 --pool 4 --name worker-2
python worker.py --port 9003 --pool 4 --name worker-3

# Terminal 4: dispatcher
python server.py --port 9000 --workers 127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003

# Terminal 5: cliente
python client.py --task fibonacci --payload 90
python client.py --task upper_text --payload '"hola mundo"'
python client.py --demo --concurrency 6
python client.py --status
```

Tareas disponibles: `fibonacci`, `prime_count`, `sum_list`, `upper_text`, `word_count`, `sleep`.

Opcional - RabbitMQ y almacenamiento: `docker compose up -d`, `pip install pika` y
`export RABBITMQ_URL=amqp://guest:guest@localhost:5672/%2F` antes de iniciar el dispatcher.
Segundo dispatcher y balanceador: `python server.py --port 9100 --workers ... --db results9100.db` y `nginx.conf`
o `haproxy.cfg` (clientes a `:8000`).

## 4. Resultados de la verificación

Probado en un entorno local con 3 workers y 1 dispatcher:

- Demo de 12 tareas con concurrencia 6: las 6 tareas `sleep 1 s` terminaron en ~1,0 s en total (en serie
  habrían sido 6 s) y se repartieron 2 por worker. Esto demuestra la ejecución concurrente.
- Tarea inexistente: devuelve `ok: false` con mensaje de error sin afectar al sistema.
- Failover: se detuvo `worker-2` durante la ejecución; las 12 tareas siguientes se atendieron por
  `worker-1` y `worker-3`, y `--status` mostró `worker-2` con `up: false`.
- Persistencia: se registraron 12 filas (11 exitosas, 1 error intencional).

No se ejecutaron en este entorno: Nginx, HAProxy, Docker Compose ni RabbitMQ (`events.py` no se probó contra un
broker real). Son archivos de referencia que conviene validar en su entorno.

## 5. Limitaciones y mejoras

- Un dispatcher que cae pierde las tareas en vuelo; mitigación: idempotencia + reintento del cliente, o
  encolar las tareas en RabbitMQ con confirmaciones (acks) en lugar de enviarlas directo a los workers.
- Un worker que expira por una tarea lenta (no por caída) se marca caído igualmente; ajustar `--task-timeout`.
- Sin TLS ni autenticación: en producción, TLS (`ssl.wrap_socket`/`SSLContext`) y tokens por cliente.
- Python y el GIL: los hilos sirven para tareas de E/S; para tareas de CPU intensivo conviene
  `ProcessPoolExecutor` o más procesos worker.
- SQLite con conexión única serializada es un reemplazo de desarrollo, no de producción.

## 6. Publicar en GitHub

```bash
git init
git add .
git commit -m "PFO 3: sistema distribuido cliente-servidor con sockets"
git branch -M main
git remote add origin https://github.com/carotramp/redes-pfo3-mcc.git
git push -u origin main
```

