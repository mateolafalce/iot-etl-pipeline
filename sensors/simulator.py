"""
IoT Sensor Simulator
====================
Simulates 6 IoT sensors (temperature, motion, door) and publishes their
readings to an MQTT broker on the configured topic prefix.

Sensors:
  - temp-01, temp-02   : temperature sensors (°C)
  - motion-01, motion-02: motion sensors (boolean 0/1)
  - door-01, door-02   : door sensors (0=closed, 1=open)

Topic pattern: sensors/<sensor_type>/<sensor_id>

Anomaly injection:
  - 5 % chance of a null value (tests null-filtering in clean step)
  - 5 % chance of an out-of-range spike for temperature sensors
  - 2 % chance of a malformed / missing timestamp
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt
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
logger = logging.getLogger("sensor_simulator")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "sensors")
PUBLISH_INTERVAL_BASE = float(os.getenv("PUBLISH_INTERVAL", "2.0"))  # seconds

# ---------------------------------------------------------------------------
# Sensor definitions
# ---------------------------------------------------------------------------
SENSORS: list[Dict[str, Any]] = [
    {
        "sensor_id": "temp-01",
        "sensor_type": "temperature",
        "unit": "celsius",
        "normal_min": 18.0,
        "normal_max": 26.0,
        "spike_min": 40.0,
        "spike_max": 80.0,
    },
    {
        "sensor_id": "temp-02",
        "sensor_type": "temperature",
        "unit": "celsius",
        "normal_min": 18.0,
        "normal_max": 26.0,
        "spike_min": -20.0,
        "spike_max": -10.0,
    },
    {
        "sensor_id": "motion-01",
        "sensor_type": "motion",
        "unit": "boolean",
        "normal_min": 0,
        "normal_max": 1,
    },
    {
        "sensor_id": "motion-02",
        "sensor_type": "motion",
        "unit": "boolean",
        "normal_min": 0,
        "normal_max": 1,
    },
    {
        "sensor_id": "door-01",
        "sensor_type": "door",
        "unit": "boolean",
        "normal_min": 0,
        "normal_max": 1,
    },
    {
        "sensor_id": "door-02",
        "sensor_type": "door",
        "unit": "boolean",
        "normal_min": 0,
        "normal_max": 1,
    },
]


# ---------------------------------------------------------------------------
# Value generators
# ---------------------------------------------------------------------------

def _is_business_hours() -> bool:
    """Return True if current UTC time falls within Mon–Fri 08:00–18:00."""
    now = datetime.now(timezone.utc)
    return now.weekday() < 5 and 8 <= now.hour < 18


def _generate_temperature(sensor: Dict[str, Any]) -> Optional[float]:
    """Generate a temperature reading, occasionally injecting anomalies."""
    roll = random.random()
    if roll < 0.05:
        # Inject null (will be caught by clean step)
        return None
    if roll < 0.10 and "spike_min" in sensor:
        # Inject out-of-range spike
        return round(random.uniform(sensor["spike_min"], sensor["spike_max"]), 2)
    return round(random.uniform(sensor["normal_min"], sensor["normal_max"]), 2)


def _generate_motion(sensor: Dict[str, Any]) -> int:
    """Generate a motion boolean, biased toward detected during business hours."""
    if _is_business_hours():
        # Higher probability of detection during business hours
        return 1 if random.random() < 0.6 else 0
    return 1 if random.random() < 0.1 else 0


def _generate_door(_sensor: Dict[str, Any]) -> int:
    """Generate a door open/close event."""
    return 1 if random.random() < 0.2 else 0


GENERATORS = {
    "temperature": _generate_temperature,
    "motion": _generate_motion,
    "door": _generate_door,
}


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def build_payload(sensor: Dict[str, Any]) -> Dict[str, Any]:
    """
    Construct the MQTT JSON payload for a sensor reading.

    Occasionally injects:
    - Missing timestamp (2 % chance)
    - Malformed timestamp string (1 % chance)
    - Missing unit field (1 % chance)
    """
    value = GENERATORS[sensor["sensor_type"]](sensor)

    # Timestamp injection failures
    ts_roll = random.random()
    if ts_roll < 0.02:
        timestamp = None  # missing
    elif ts_roll < 0.03:
        timestamp = "not-a-timestamp"  # malformed
    else:
        timestamp = datetime.now(timezone.utc).isoformat()

    payload: Dict[str, Any] = {
        "sensor_id": sensor["sensor_id"],
        "sensor_type": sensor["sensor_type"],
        "value": value,
        "timestamp": timestamp,
    }

    # Occasionally omit the unit field
    if random.random() >= 0.01:
        payload["unit"] = sensor["unit"]

    return payload


# ---------------------------------------------------------------------------
# MQTT client setup
# ---------------------------------------------------------------------------

def on_connect(client: mqtt.Client, userdata: Any, flags: Any, rc: int) -> None:
    if rc == 0:
        logger.info("Connected to MQTT broker at %s:%d", MQTT_BROKER, MQTT_PORT)
    else:
        logger.error("Failed to connect to MQTT broker. Return code: %d", rc)


def on_publish(client: mqtt.Client, userdata: Any, mid: int) -> None:
    logger.debug("Message mid=%d acknowledged by broker", mid)


def create_mqtt_client() -> mqtt.Client:
    """Create, configure and connect an MQTT client."""
    client = mqtt.Client(client_id="sensor-simulator")
    client.on_connect = on_connect
    client.on_publish = on_publish
    return client


def connect_with_retry(client: mqtt.Client, max_attempts: int = 10) -> None:
    """Attempt broker connection with exponential back-off."""
    delay = 2.0
    for attempt in range(1, max_attempts + 1):
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_start()
            return
        except (ConnectionRefusedError, OSError) as exc:
            logger.warning(
                "Attempt %d/%d – cannot connect to broker (%s). Retrying in %.1fs…",
                attempt,
                max_attempts,
                exc,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError(
        f"Could not connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT} "
        f"after {max_attempts} attempts."
    )


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_running = True


def _handle_sigterm(signum: int, frame: Any) -> None:
    global _running
    logger.info("Received signal %d – shutting down gracefully…", signum)
    _running = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


# ---------------------------------------------------------------------------
# Main publish loop
# ---------------------------------------------------------------------------

def run() -> None:
    """Main entry-point: connect and publish sensor readings indefinitely."""
    client = create_mqtt_client()
    connect_with_retry(client)

    logger.info(
        "Simulator started. Publishing %d sensors every ~%.1fs …",
        len(SENSORS),
        PUBLISH_INTERVAL_BASE,
    )

    while _running:
        for sensor in SENSORS:
            if not _running:
                break

            payload = build_payload(sensor)
            topic = f"{MQTT_TOPIC_PREFIX}/{sensor['sensor_type']}/{sensor['sensor_id']}"
            message = json.dumps(payload)

            result = client.publish(topic, message, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(
                    "Published → %s | value=%s ts=%s",
                    topic,
                    payload.get("value"),
                    payload.get("timestamp"),
                )
            else:
                logger.warning("Failed to publish to %s (rc=%d)", topic, result.rc)

            # Small random jitter between individual sensor publishes
            time.sleep(PUBLISH_INTERVAL_BASE + random.uniform(-0.5, 0.5))

        # Brief pause between full sensor rounds
        time.sleep(random.uniform(0.5, 1.5))

    logger.info("Stopping MQTT loop…")
    client.loop_stop()
    client.disconnect()
    logger.info("Simulator stopped.")
    sys.exit(0)


if __name__ == "__main__":
    run()
