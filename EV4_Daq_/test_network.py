#!/usr/bin/env python3
"""
Network diagnostic: measure raw write latency/reliability to InfluxDB.

This bypasses the logger's batching entirely and does N plain, sequential HTTP
writes to the InfluxDB v3 write endpoint, timing each one. It tells you whether
your *network link* can talk to InfluxDB quickly and reliably -- which separates
a "bad connection" problem from a "code" problem.

    source venv/bin/activate
    python3 test_network.py

Reading the results:
  - HTTP 204, low latency (<300 ms), all succeed  -> network is fine; the
    real-time lag is in the code/config, not the link.
  - High latency (>1 s), timeouts, or non-204 codes -> the link is the
    bottleneck. Streaming full-rate CAN to the cloud over it won't be real-time;
    you need to downsample or upload the CSV after the session.
"""
import os
import sys
import time
import socket
import urllib.parse
import urllib.request
import urllib.error

from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("INFLUX_HOST", "https://us-east-1-1.aws.cloud2.influxdata.com").rstrip("/")
TOKEN = os.getenv("INFLUX_TOKEN")
ORG = os.getenv("INFLUX_ORG", "EV 4")
DATABASE = os.getenv("INFLUX_DATABASE", "ev4_can_data")

N = int(os.getenv("NET_TEST_COUNT", "20"))
TIMEOUT = float(os.getenv("NET_TEST_TIMEOUT", "15"))

if not TOKEN:
    print("ERROR: INFLUX_TOKEN not set in .env")
    sys.exit(1)

url = f"{HOST}/api/v2/write?" + urllib.parse.urlencode(
    {"bucket": DATABASE, "org": ORG, "precision": "ns"}
)

print(f"Target : {HOST}")
print(f"Writing {N} single points to measurement 'debug_net' (timeout {TIMEOUT:.0f}s each)")

# DNS resolution timing (a slow/failing DNS is its own kind of bad link) -------
hostname = urllib.parse.urlparse(HOST).hostname
try:
    t = time.perf_counter()
    ip = socket.gethostbyname(hostname)
    print(f"DNS    : {hostname} -> {ip}  ({(time.perf_counter()-t)*1000:.0f} ms)")
except Exception as e:
    print(f"DNS    : FAILED to resolve {hostname}: {e}")
print("-" * 60)

latencies = []
ok = 0
for i in range(1, N + 1):
    body = f"debug_net,host=pi value={i}i {time.time_ns()}".encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Authorization": f"Token {TOKEN}",
            "Content-Type": "text/plain; charset=utf-8",
        },
    )
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            dt = time.perf_counter() - start
            latencies.append(dt)
            if resp.status in (200, 204):
                ok += 1
            print(f"[{i:2d}/{N}] HTTP {resp.status}   {dt*1000:7.0f} ms")
    except urllib.error.HTTPError as e:
        dt = time.perf_counter() - start
        print(f"[{i:2d}/{N}] HTTP {e.code} ERROR {dt*1000:7.0f} ms  {e.read()[:120]!r}")
    except (urllib.error.URLError, socket.timeout) as e:
        dt = time.perf_counter() - start
        print(f"[{i:2d}/{N}] FAILED/timeout after {dt*1000:7.0f} ms  {e}")
    time.sleep(0.5)

print("-" * 60)
if latencies:
    latencies.sort()
    median = latencies[len(latencies) // 2]
    print(f"Success: {ok}/{N}   "
          f"min {min(latencies)*1000:.0f} ms | "
          f"median {median*1000:.0f} ms | "
          f"max {max(latencies)*1000:.0f} ms")
else:
    print(f"Success: {ok}/{N}   (no successful writes timed)")

if ok == N and latencies and latencies[len(latencies)//2] < 0.3:
    print("\nVERDICT: network looks healthy -> investigate code/config, not the link.")
elif ok < N:
    print("\nVERDICT: writes are failing -> NETWORK is the problem (or a bad token).")
else:
    print("\nVERDICT: writes succeed but are SLOW -> the link is the bottleneck.")
