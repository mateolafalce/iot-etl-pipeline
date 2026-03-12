"""
MQTT Consumer / Ingestor
========================
Subscribes to the ``sensors/#`` MQTT topic tree and persists every received
message as a row in the ``raw_events`` PostgreSQL table.

Features
--------
- Automatic reconnection with exponential back-off for both MQTT and Postgres.
- Stores the raw JSON payload in a JSONB column so nothing is lost upstream.
- Thread-safe: DB writes happen in the MQTT callback thread; a connection pool
  (single persistent connection with auto-reconnect) keeps things simple while
  remaining robust for moderate throughput.
- Structured logging on every event.
"""

import json
import logging
import os
import signal
import sys
import time
from typing import Any, Optional

import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("mqtt_consumer")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "sensors")
SUBSCRIBE_TOPIC = f"{MQTT_TOPIC_PREFIX}/#"

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "iot_db")
PG_USER = os.getenv("POSTGRES_USER", "iot_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iot_pass")

INSERT_SQL = """
    INSERT INTO raw_events (sensor_id, sensor_type, payload)
    VALUES (%(sensor_id)s, %(sensor_type)s, %(payload)s)
"""


# ---------------------------------------------------------------------------
# PostgreSQL connection manager
# ---------------------------------------------------------------------------

class DatabaseConnection:
    """Wrapper around a single psycopg2 connection with auto-reconnect."""

    def __init__(self) -> None:
        self._conn: Optional[psycopg2.extensions.connection] = None

    # ------------------------------------------------------------------
    def _dsn(self) -> str:
        return (
            f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
            f"user={PG_USER} password={PG_PASSWORD} "
            "connect_timeout=10"
        )

    # ------------------------------------------------------------------
    def connect(self, max_attempts: int = 10) -> None:
        """Establish the database connection, retrying with back-off."""
        delay = 2.0
        for attempt in range(1, max_attempts + 1):
            try:
                self._conn = psycopg2.connect(self._dsn())
                self._conn.autocommit = True
                logger.info(
                    "Connected to PostgreSQL at %s:%d/%s", PG_HOST, PG_PORT, PG_DB
                )
                return
            except psycopg2.OperationalError as exc:
                logger.warning(
                    "DB connect attempt %d/%d failed: %s. Retry in %.1fs…",
                    attempt,
                    max_attempts,
                    exc,
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
        raise RuntimeError(
            f"Cannot connect to PostgreSQL at {PG_HOST}:{PG_PORT} after "
            f"{max_attempts} attempts."
        )

    # ------------------------------------------------------------------
    def execute(self, sql: str, params: dict) -> None:
        """Execute a parameterised SQL statement, reconnecting if needed."""
        for attempt in range(1, 4):
            try:
                if self._conn is None or self._conn.closed:
                    logger.warning("DB connection lost – reconnecting…")
                    self.connect()
                with self._conn.cursor() as cur:
                    cur.execute(sql, params)
                return
            except psycopg2.OperationalError as exc:
                logger.error("DB execute error (attempt %d/3): %s", attempt, exc)
                self._conn = None
                time.sleep(2.0 ** attempt)
        logger.error("Giving up on DB write after 3 attempts.")

    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("PostgreSQL connection closed.")


db = DatabaseConnection()


# ---------------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------------

def on_connect(
    client: mqtt.Client, userdata: Any, flags: Any, rc: int
) -> None:
    """Called when the client connects (or reconnects) to the broker."""
    if rc == 0:
        logger.info(
            "Connected to MQTT broker at %s:%d. Subscribing to '%s'.",
            MQTT_BROKER,
            MQTT_PORT,
            SUBSCRIBE_TOPIC,
        )
        client.subscribe(SUBSCRIBE_TOPIC, qos=1)
    else:
        logger.error(
            "MQTT connection failed. Return code: %d – will retry automatically.", rc
        )


def on_disconnect(client: mqtt.Client, userdata: Any, rc: int) -> None:
    """Called when the client disconnects from the broker."""
    if rc != 0:
        logger.warning(
            "Unexpected MQTT disconnect (rc=%d). Paho will attempt to reconnect.", rc
        )
    else:
        logger.info("MQTT client disconnected cleanly.")


def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    """
    Callback for every incoming MQTT message.

    Parses the JSON payload and writes a row to ``raw_events``.  If JSON
    parsing fails the raw bytes are still persisted inside the payload column
    under the ``raw`` key so the event is not silently dropped.
    """
    topic = msg.topic
    raw_bytes = msg.payload

    logger.info("Received message on topic '%s' (%d bytes)", topic, len(raw_bytes))

    # ------------------------------------------------------------------
    # Parse JSON (best-effort)
    # ------------------------------------------------------------------
    try:
        payload_dict = json.loads(raw_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning(
            "Could not parse JSON from topic '%s': %s. Storing raw bytes.", topic, exc
        )
        payload_dict = {"raw": raw_bytes.decode("utf-8", errors="replace")}

    # ------------------------------------------------------------------
    # Extract top-level fields for indexed columns (best-effort)
    # ------------------------------------------------------------------
    sensor_id: Optional[str] = None
    sensor_type: Optional[str] = None

    if isinstance(payload_dict, dict):
        sensor_id = payload_dict.get("sensor_id")
        sensor_type = payload_dict.get("sensor_type")

    # Fall back to deriving sensor_type from topic parts if missing in payload
    # Topic pattern: sensors/<sensor_type>/<sensor_id>
    topic_parts = topic.split("/")
    if len(topic_parts) >= 3:
        if sensor_type is None:
            sensor_type = topic_parts[1]
        if sensor_id is None:
            sensor_id = topic_parts[2]

    # ------------------------------------------------------------------
    # Write to raw_events
    # ------------------------------------------------------------------
    db.execute(
        INSERT_SQL,
        {
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "payload": psycopg2.extras.Json(payload_dict),
        },
    )
    logger.info(
        "Stored raw event → sensor_id=%s sensor_type=%s", sensor_id, sensor_type
    )


# ---------------------------------------------------------------------------
# MQTT client factory
# ---------------------------------------------------------------------------

def create_client() -> mqtt.Client:
    """Build and configure the Paho MQTT client."""
    client = mqtt.Client(client_id="mqtt-consumer")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    # Paho will automatically attempt to reconnect on unexpected disconnect
    client.reconnect_delay_set(min_delay=2, max_delay=60)
    return client


def connect_mqtt_with_retry(client: mqtt.Client, max_attempts: int = 15) -> None:
    """Connect to the MQTT broker with exponential back-off."""
    delay = 2.0
    for attempt in range(1, max_attempts + 1):
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            return
        except (ConnectionRefusedError, OSError) as exc:
            logger.warning(
                "MQTT connect attempt %d/%d failed: %s. Retry in %.1fs…",
                attempt,
                max_attempts,
                exc,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError(
        f"Cannot connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT} after "
        f"{max_attempts} attempts."
    )


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_running = True
_mqtt_client: Optional[mqtt.Client] = None


def _handle_signal(signum: int, frame: Any) -> None:
    global _running
    logger.info("Received signal %d – initiating graceful shutdown…", signum)
    _running = False
    if _mqtt_client is not None:
        _mqtt_client.loop_stop()
        _mqtt_client.disconnect()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    global _mqtt_client

    # 1. Establish DB connection first
    db.connect()

    # 2. Create and connect MQTT client
    _mqtt_client = create_client()
    connect_mqtt_with_retry(_mqtt_client)

    logger.info("Starting MQTT network loop…")
    _mqtt_client.loop_start()

    # 3. Keep process alive until signal received
    while _running:
        time.sleep(1)

    # 4. Cleanup
    logger.info("Shutting down…")
    _mqtt_client.loop_stop()
    _mqtt_client.disconnect()
    db.close()
    logger.info("MQTT consumer stopped.")
    sys.exit(0)


if __name__ == "__main__":
    run()
