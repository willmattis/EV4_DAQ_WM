#!/usr/bin/env python3
"""
EV4 DAQ - Live CAN -> InfluxDB v3 logger.

Reads CAN frames from a socketcan interface (e.g. can0) on the Raspberry Pi,
decodes them with the DBC files in ./DBC, writes every decoded signal to an
InfluxDB Cloud v3 database in real time, and keeps a local CSV backup of every
raw frame so nothing is lost if the network drops.

Uses the InfluxDB 3 client (influxdb3-python / InfluxDBClient3). Config comes
from a .env file (see .env.example). Run with:
    python3 can_to_influx.py
Stop with Ctrl+C.
"""
import os
import sys
import csv
import time
import signal
import logging
import platform
import threading
from pathlib import Path
from datetime import datetime, timezone

import can
import cantools
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, Point, write_client_options
from influxdb_client_3.write_client.client.write_api import WriteOptions, WriteType

# ---------------------------------------------------------------------------
# Configuration (from .env)
# ---------------------------------------------------------------------------
load_dotenv()

INFLUX_HOST = os.getenv("INFLUX_HOST", "https://us-east-1-1.aws.cloud2.influxdata.com")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "EV 4")
INFLUX_DATABASE = os.getenv("INFLUX_DATABASE", "ev4_can_data")

CAN_CHANNEL = os.getenv("CAN_CHANNEL", "can0")
CAN_INTERFACE = os.getenv("CAN_INTERFACE", "socketcan")  # "socketcan" on the Pi

DBC_DIR = Path(os.getenv("DBC_DIR", "DBC"))
LOG_DIR = Path(os.getenv("LOG_DIR", "can_logs"))

# Optional tag applied to every point so you can filter runs in InfluxDB.
SESSION_TAG = os.getenv("SESSION_TAG", "")

# ---------------------------------------------------------------------------
# Live-upload policy (for weak/cellular links)
# ---------------------------------------------------------------------------
# The CAN bus produces far more data than a hotspot can stream (the inverter
# messages alone flood the link). The CSV backup always captures EVERY frame at
# full rate, so nothing is ever lost -- this policy only controls what gets
# uploaded to InfluxDB *live*. Run backfill_csv.py later to push the full CSV
# over a good connection.
#
# Each entry is: measurement name -> max live upload rate in Hz.
#   value > 0   : cap to that many uploads/second (downsample)
#   value <= 0  : drop from live entirely (CSV-only)
# Anything not listed uses LIVE_DEFAULT_HZ. Default is 0, so by default only the
# messages listed here go up live -- an allowlist, ordered by your priorities.
LIVE_DEFAULT_HZ = float(os.getenv("LIVE_DEFAULT_HZ", "0"))
LIVE_RATES_HZ = {
    "ECU_FAULTS":       1000.0,   # 1) faults: critical, low-rate -> every frame
    "BMS_Info":           10.0,   # 2) pack SOC / current / temps
    "APPS_Info":          15.0,   # 3) pedal & torque command
    "Internal_States":    10.0,   # 4) power draw, R2D, drive mode
    "Sensors_Info":        5.0,   # 5) brake pressure, water temps
}

# Tracks the last live-upload time per measurement, for rate limiting.
_last_live: dict[str, float] = {}


def allow_live(name: str, now: float) -> bool:
    """Rate-limit/allowlist gate for live upload. CSV logging is unaffected."""
    hz = LIVE_RATES_HZ.get(name, LIVE_DEFAULT_HZ)
    if hz <= 0:
        return False
    if now - _last_live.get(name, 0.0) >= 1.0 / hz:
        _last_live[name] = now
        return True
    return False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ev4_daq")

running = True
_sigint_count = 0


def signal_handler(signum, frame):
    global running, _sigint_count
    _sigint_count += 1
    if _sigint_count == 1:
        logger.info("Shutdown signal received, stopping (flushing backlog -- "
                    "press Ctrl+C again to force-quit immediately)...")
        running = False
    else:
        logger.warning("Second Ctrl+C -- force quitting. Unflushed points remain "
                       "in the CSV backup.")
        os._exit(1)


def load_dbc() -> cantools.database.Database:
    """Load and merge every .dbc file in DBC_DIR into one database."""
    if not DBC_DIR.exists():
        logger.error("DBC directory '%s' not found.", DBC_DIR)
        sys.exit(1)

    dbc_files = sorted(DBC_DIR.glob("*.dbc"))
    if not dbc_files:
        logger.error("No .dbc files found in '%s'.", DBC_DIR)
        sys.exit(1)

    db = cantools.database.Database()
    for dbc_file in dbc_files:
        logger.info("Loading DBC: %s", dbc_file.name)
        temp = cantools.database.load_file(str(dbc_file))
        for msg in temp.messages:
            if not any(m.frame_id == msg.frame_id for m in db.messages):
                db.messages.append(msg)
    db.refresh()
    logger.info("Loaded %d CAN message definitions.", len(db.messages))
    return db


def setup_bus() -> can.BusABC:
    """Open the CAN bus. On the Pi this is socketcan/can0."""
    if CAN_INTERFACE == "socketcan" and platform.system() != "Linux":
        logger.warning(
            "socketcan only works on Linux (the Pi). Current OS is %s; "
            "set CAN_INTERFACE in .env for local testing.",
            platform.system(),
        )
    try:
        bus = can.interface.Bus(channel=CAN_CHANNEL, interface=CAN_INTERFACE)
        logger.info("CAN bus open: %s (%s)", CAN_CHANNEL, CAN_INTERFACE)
        return bus
    except Exception as e:
        logger.error("Failed to open CAN bus: %s", e)
        logger.error("On the Pi, bring the interface up first:")
        logger.error("  sudo ip link set can0 type can bitrate 500000")
        logger.error("  sudo ip link set up can0")
        sys.exit(1)


# Running totals of what has actually been confirmed written to InfluxDB
# (updated by the batching write callbacks on a background thread).
upload_stats = {"points": 0, "batches": 0, "errors": 0}


def _count_points(data) -> int:
    """Number of line-protocol points in a flushed batch payload."""
    text = data.decode() if isinstance(data, (bytes, bytearray)) else data
    return len(text.splitlines())


def _safe_close(client):
    """Close the client (flushes queued batches). Run on a worker thread so a
    slow network can be bounded by a join timeout in the caller."""
    try:
        client.close()
    except Exception as e:
        logger.error("Error during InfluxDB flush/close: %s", e)


# Batching write callbacks. With the batching write API, writes are queued and
# flushed on a background thread, so success/failure is reported here (errors
# from a bad token/database/network show up a second or two after the first write).
def _success_cb(conf, data):
    n = _count_points(data)
    upload_stats["points"] += n
    upload_stats["batches"] += 1
    logger.info("InfluxDB <- uploaded %d points (%d total).", n, upload_stats["points"])


def _error_cb(conf, data, exception):
    upload_stats["errors"] += 1
    logger.error("InfluxDB write FAILED (%d points dropped): %s",
                 _count_points(data), exception)


def _retry_cb(conf, data, exception):
    logger.warning("Retrying InfluxDB write after error: %s", exception)


def make_influx_client() -> InfluxDBClient3:
    """Create the InfluxDB v3 client with batching enabled."""
    if not INFLUX_TOKEN:
        logger.error("INFLUX_TOKEN is not set. Copy .env.example to .env and fill it in.")
        sys.exit(1)

    write_options = WriteOptions(
        batch_size=5_000,        # large batches amortize the per-request latency
        flush_interval=2_000,    # ...flushed at least every 2 s (ms)
        jitter_interval=200,
        retry_interval=5_000,
        max_retries=5,
        max_retry_delay=30_000,
        exponential_base=2,
        write_type=WriteType.batching,
    )
    wco = write_client_options(
        success_callback=_success_cb,
        error_callback=_error_cb,
        retry_callback=_retry_cb,
        write_options=write_options,
    )

    client = InfluxDBClient3(
        host=INFLUX_HOST,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG,
        database=INFLUX_DATABASE,
        write_client_options=wco,
        enable_gzip=True,        # compress line protocol (~5-10x) for slow links
    )
    logger.info("InfluxDB v3 client ready -> database '%s' on %s",
                INFLUX_DATABASE, INFLUX_HOST)
    logger.info("Batching: flush every 1 s or 200 points. You'll see "
                "'InfluxDB <- uploaded N points' lines once data flows.")
    return client


def build_point(message, decoded: dict, raw_id: int, ts_unix: float) -> Point | None:
    """Turn a decoded CAN message into an InfluxDB Point. None if no usable fields."""
    point = Point(message.name).tag("raw_id", f"0x{raw_id:X}")
    if SESSION_TAG:
        point = point.tag("session", SESSION_TAG)

    has_field = False
    for name, value in decoded.items():
        # cantools returns numbers for normal signals and NamedSignalValue for
        # enum signals. Store numbers as floats, enums/strings as strings.
        if isinstance(value, bool):
            point = point.field(name, int(value))
            has_field = True
        elif isinstance(value, (int, float)):
            point = point.field(name, float(value))
            has_field = True
        else:
            point = point.field(name, str(value))
            has_field = True

    if not has_field:
        return None

    # Use the kernel hardware timestamp from socketcan. A tz-aware datetime is
    # serialized to nanosecond-precision line protocol by the client.
    point = point.time(datetime.fromtimestamp(ts_unix, tz=timezone.utc))
    return point


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    db = load_dbc()
    bus = setup_bus()
    client = make_influx_client()

    LOG_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = LOG_DIR / f"can_{stamp}.csv"
    logger.info("CSV backup: %s", csv_path)

    decoded_count = 0   # all messages decoded (all of which go to the CSV)
    queued_count = 0    # subset actually sent to InfluxDB live (after the policy)
    unknown_ids = set()
    last_stats = time.monotonic()

    try:
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["timestamp_iso", "timestamp_unix", "id_hex", "dlc",
                 "data_hex", "message_name", "decoded_signals"]
            )

            while running:
                # Every 5 s, print decoded vs uploaded vs backlog. A backlog that
                # keeps growing means the network can't keep up with the data rate
                # (network problem); a flat/zero backlog means uploads keep pace.
                now = time.monotonic()
                if now - last_stats >= 5.0:
                    # backlog = queued-for-live minus confirmed-uploaded. If it
                    # keeps growing, the link still can't keep up even after the
                    # downsample policy -> tighten LIVE_RATES_HZ. decoded>>queued
                    # is expected and fine (the rest is CSV-only / back-filled).
                    backlog = queued_count - upload_stats["points"]
                    logger.info(
                        "STATS decoded=%d queued_live=%d uploaded=%d backlog=%d errors=%d",
                        decoded_count, queued_count, upload_stats["points"],
                        backlog, upload_stats["errors"],
                    )
                    last_stats = now

                msg = bus.recv(timeout=1.0)
                if msg is None:
                    continue

                ts_unix = msg.timestamp  # unix float from socketcan
                ts_iso = datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat()
                data_hex = msg.data.hex()

                message = None
                decoded = None
                try:
                    message = db.get_message_by_frame_id(msg.arbitration_id)
                    decoded = db.decode_message(msg.arbitration_id, msg.data)
                except KeyError:
                    if msg.arbitration_id not in unknown_ids:
                        unknown_ids.add(msg.arbitration_id)
                        logger.debug("No DBC entry for ID 0x%X", msg.arbitration_id)
                except Exception as e:
                    logger.warning("Decode failed for 0x%X: %s", msg.arbitration_id, e)

                msg_name = message.name if message else "Unknown"
                decoded_str = (
                    ", ".join(f"{k}={v}" for k, v in decoded.items()) if decoded else ""
                )

                writer.writerow(
                    [ts_iso, f"{ts_unix:.6f}", f"0x{msg.arbitration_id:X}",
                     msg.dlc, data_hex, msg_name, decoded_str]
                )
                f.flush()

                if message and decoded:
                    decoded_count += 1
                    # Live-upload policy: only the prioritized messages, rate-
                    # limited, go up live. Everything else is CSV-only and gets
                    # back-filled later. (CSV write above already captured it.)
                    if allow_live(msg_name, now):
                        point = build_point(message, decoded, msg.arbitration_id, ts_unix)
                        if point is not None:
                            client.write(record=point)
                            queued_count += 1
                            # Upload confirmation is logged by _success_cb in real
                            # time as each batch flushes.

    except Exception as e:
        logger.error("Fatal error in main loop: %s", e)
    finally:
        backlog = queued_count - upload_stats["points"]
        logger.info("Flushing remaining InfluxDB writes (backlog ~%d points)...", backlog)
        # Flush on a background thread so a slow/dead network can't hang shutdown
        # forever. Everything is already safe in the CSV, so a timeout is OK.
        closer = threading.Thread(target=_safe_close, args=(client,), daemon=True)
        closer.start()
        closer.join(timeout=float(os.getenv("FLUSH_TIMEOUT", "15")))
        if closer.is_alive():
            logger.warning("Flush didn't finish in time over this network. "
                           "Exiting anyway -- the CSV backup has every frame; "
                           "you can back-fill InfluxDB from it later.")
        bus.shutdown()
        logger.info(
            "Done. Decoded %d msgs (all in CSV) | queued %d live | uploaded %d "
            "in %d batches | write errors: %d | unknown IDs: %d.",
            decoded_count, queued_count, upload_stats["points"],
            upload_stats["batches"], upload_stats["errors"], len(unknown_ids),
        )
        logger.info("Full-resolution data is in %s -- run "
                    "'python3 backfill_csv.py %s' over a good connection to upload it all.",
                    csv_path, csv_path)


if __name__ == "__main__":
    main()
