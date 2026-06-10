#!/usr/bin/env python3
"""
Back-fill InfluxDB from a CAN CSV log -- the full-resolution counterpart to the
live logger.

can_to_influx.py only uploads the prioritized messages live (to survive a weak
link) but writes EVERY frame to a CSV in can_logs/. Run this afterward, over a
good connection, to push the entire log into InfluxDB at full resolution.

It re-decodes each row from the raw hex using the same DBC files and the same
build_point() as the live logger, so the data lands identically -- just complete.

    source venv/bin/activate
    python3 backfill_csv.py can_logs/can_20260610_024931.csv

Options:
    --only ECU_FAULTS,BMS_Info   only upload these measurements
    --skip M173_...,M172_...     upload everything except these
Re-running the same file is safe: InfluxDB de-duplicates points with identical
measurement+tags+timestamp, so you won't get doubles.
"""
import os
import sys
import csv
import time
import argparse

from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, write_client_options
from influxdb_client_3.write_client.client.write_api import WriteOptions, WriteType

# Reuse the exact decode + point-building logic from the live logger.
import can_to_influx as live

load_dotenv()

HOST = os.getenv("INFLUX_HOST", "https://us-east-1-1.aws.cloud2.influxdata.com")
TOKEN = os.getenv("INFLUX_TOKEN")
ORG = os.getenv("INFLUX_ORG", "EV 4")
DATABASE = os.getenv("INFLUX_DATABASE", "ev4_can_data")

uploaded = {"points": 0, "batches": 0, "errors": 0}


def _success(conf, data):
    uploaded["points"] += len(data.decode().splitlines()) if isinstance(data, (bytes, bytearray)) \
        else len(data.splitlines())
    uploaded["batches"] += 1


def _error(conf, data, exception):
    uploaded["errors"] += 1
    print(f"  write error: {exception}")


def main():
    ap = argparse.ArgumentParser(description="Back-fill InfluxDB from a CAN CSV log.")
    ap.add_argument("csv_file", help="Path to a can_logs/*.csv file")
    ap.add_argument("--only", default="", help="Comma-separated measurements to include")
    ap.add_argument("--skip", default="", help="Comma-separated measurements to exclude")
    args = ap.parse_args()

    if not TOKEN:
        print("ERROR: INFLUX_TOKEN not set in .env")
        sys.exit(1)
    if not os.path.exists(args.csv_file):
        print(f"ERROR: file not found: {args.csv_file}")
        sys.exit(1)

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}

    db = live.load_dbc()

    write_options = WriteOptions(
        batch_size=5_000, flush_interval=2_000, jitter_interval=200,
        retry_interval=5_000, max_retries=5, max_retry_delay=30_000,
        exponential_base=2, write_type=WriteType.batching,
    )
    wco = write_client_options(success_callback=_success, error_callback=_error,
                               write_options=write_options)
    client = InfluxDBClient3(host=HOST, token=TOKEN, org=ORG, database=DATABASE,
                             write_client_options=wco, enable_gzip=True)
    print(f"Back-filling {args.csv_file} -> database '{DATABASE}' on {HOST}")
    if only:
        print(f"  including only: {sorted(only)}")
    if skip:
        print(f"  skipping: {sorted(skip)}")

    rows = sent = skipped_unknown = filtered = 0
    t0 = time.time()
    try:
        with open(args.csv_file, newline="") as f:
            for row in csv.DictReader(f):
                rows += 1
                name = row.get("message_name", "")
                if name == "Unknown" or not name:
                    skipped_unknown += 1
                    continue
                if only and name not in only:
                    filtered += 1
                    continue
                if name in skip:
                    filtered += 1
                    continue
                try:
                    raw_id = int(row["id_hex"], 16)
                    data = bytes.fromhex(row["data_hex"])
                    ts_unix = float(row["timestamp_unix"])
                    message = db.get_message_by_frame_id(raw_id)
                    decoded = db.decode_message(raw_id, data)
                except Exception:
                    skipped_unknown += 1
                    continue
                point = live.build_point(message, decoded, raw_id, ts_unix)
                if point is not None:
                    client.write(record=point)
                    sent += 1
                    if sent % 20_000 == 0:
                        print(f"  ...queued {sent} points (row {rows})")
    finally:
        print("Flushing final batches (this can take a moment)...")
        client.close()

    dt = time.time() - t0
    print("-" * 60)
    print(f"Rows read:        {rows}")
    print(f"Queued for write: {sent}")
    print(f"Uploaded:         {uploaded['points']} in {uploaded['batches']} batches")
    print(f"Skipped unknown:  {skipped_unknown}")
    print(f"Filtered out:     {filtered}")
    print(f"Write errors:     {uploaded['errors']}")
    print(f"Took {dt:.1f}s")
    if uploaded["errors"]:
        print("\nSome batches failed -- re-run the same file to fill the gaps "
              "(InfluxDB de-dupes identical points, so it's safe).")


if __name__ == "__main__":
    main()
