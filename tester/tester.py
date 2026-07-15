#!/usr/bin/env python3
"""
GT06 Ingest Hackathon - official tester / load generator / verifier.

Speaks the GT06 subset defined in PROBLEM.md (login 0x01 + position 0x12),
drives a target TCP server with simulated devices, verifies protocol
responses and the rows the server stored in MySQL.

Modes:
  selftest  - verify the tester's own protocol vectors (no server needed)
  basic     - functional correctness (few devices, happy path)   [25 pts]
  stream    - TCP stream robustness (fragmentation, corruption)  [20 pts]
  load      - concurrency + throughput + storage + conn budget   [45 pts]
  ramp      - device-count ladder, finds max concurrent devices  [leaderboard]
  traccar   - compatibility mode to benchmark a real Traccar server
  all       - basic + stream + load

Examples:
  python tester.py basic  --host 127.0.0.1 --port 5023
  python tester.py load   --devices 10000 --interval 1 --duration 30
  python tester.py ramp   --ladder 1000,2000,5000,10000,15000,20000
  python tester.py traccar --devices 500 --duration 30 --mysql-db traccar

Requires: Python 3.10+, pip install pymysql
"""

import argparse
import asyncio
import multiprocessing
import os
import random
import socket
import struct
import sys
import threading
import time
from datetime import datetime, timezone

try:
    import pymysql
    import pymysql.cursors
except ImportError:
    pymysql = None

# ----------------------------------------------------------------------------
# protocol layer (GT06 subset)
# ----------------------------------------------------------------------------

HEADER = b"\x78\x78"
TRAILER = b"\x0d\x0a"
PROTO_LOGIN = 0x01
PROTO_POSITION = 0x12


def _make_crc_table():
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ 0x8408 if crc & 1 else crc >> 1
        table.append(crc)
    return table


_CRC_TABLE = _make_crc_table()


def crc16_x25(data) -> int:
    """CRC-ITU (X-25): poly 0x1021 reflected, init 0xFFFF, final xor 0xFFFF."""
    crc = 0xFFFF
    tbl = _CRC_TABLE
    for b in data:
        crc = (crc >> 8) ^ tbl[(crc ^ b) & 0xFF]
    return (crc ^ 0xFFFF) & 0xFFFF


def imei_to_bcd(imei: str) -> bytes:
    """15-digit IMEI -> 8 bytes BCD (left-padded with one zero nibble)."""
    s = "0" + imei
    return bytes((int(s[i]) << 4) | int(s[i + 1]) for i in range(0, 16, 2))


def build_frame(proto: int, serial: int, payload: bytes = b"") -> bytes:
    """78 78 | LEN | PROTO | PAYLOAD | SERIAL(2) | CRC(2) | 0D 0A
    LEN counts PROTO..CRC inclusive. CRC covers LEN..SERIAL inclusive."""
    body = bytes([len(payload) + 5, proto]) + payload + serial.to_bytes(2, "big")
    return HEADER + body + crc16_x25(body).to_bytes(2, "big") + TRAILER


def build_login(imei: str, serial: int) -> bytes:
    return build_frame(PROTO_LOGIN, serial, imei_to_bcd(imei))


def encode_position(serial, dt, lat, lon, speed, course, sats, valid,
                    mcc=404, mnc=45, lac=0x1A2B, cid=0x00C3D4):
    """Build a 0x12 position frame. lat/lon in signed degrees.
    Returns (frame_bytes, expected_db_fields) where fields are exactly what
    a correct server must store: (fix_time, valid, latitude, longitude,
    speed, course, satellites)."""
    lat_raw = round(abs(lat) * 1800000.0)
    lon_raw = round(abs(lon) * 1800000.0)
    flags = course & 0x3FF
    if valid:
        flags |= 1 << 12
    if lon < 0:
        flags |= 1 << 11  # west
    if lat >= 0:
        flags |= 1 << 10  # north
    payload = struct.pack(
        ">BBBBBBBIIBHHBHBH",
        dt.year - 2000, dt.month, dt.day, dt.hour, dt.minute, dt.second,
        (0xC << 4) | (sats & 0x0F),
        lat_raw, lon_raw,
        speed & 0xFF,
        flags,
        mcc, mnc, lac,
        (cid >> 16) & 0xFF, cid & 0xFFFF,
    )
    exp_lat = lat_raw / 1800000.0 if lat >= 0 else -lat_raw / 1800000.0
    exp_lon = -lon_raw / 1800000.0 if lon < 0 else lon_raw / 1800000.0
    fields = (dt, 1 if valid else 0, exp_lat, exp_lon,
              speed & 0xFF, course & 0x3FF, sats & 0x0F)
    return build_frame(PROTO_POSITION, serial, payload), fields


def corrupt_frame(frame: bytes) -> bytes:
    """Flip one payload byte; header/len stay intact so framing survives
    but the CRC no longer matches."""
    b = bytearray(frame)
    b[6] ^= 0xFF  # a payload byte (offset 6 is inside payload for both types)
    return bytes(b)


def extract_frames(buf: bytearray):
    """Parse complete frames out of buf (mutates buf). Returns (frames, garbage)
    where frames is a list of (proto, serial, payload) with CRC verified and
    garbage counts skipped/invalid bytes."""
    out = []
    garbage = 0
    pos = 0
    n = len(buf)
    while True:
        j = buf.find(HEADER, pos)
        if j == -1:
            keep = 1 if n > 0 and buf[n - 1] == 0x78 else 0
            garbage += max(0, (n - keep) - pos)
            del buf[: n - keep]
            return out, garbage
        garbage += j - pos
        if n - j < 3:
            del buf[:j]
            return out, garbage
        ln = buf[j + 2]
        total = ln + 5
        if ln < 5:
            pos = j + 2
            garbage += 2
            continue
        if n - j < total:
            del buf[:j]
            return out, garbage
        crc_ok = crc16_x25(buf[j + 2 : j + ln + 1]) == int.from_bytes(
            buf[j + ln + 1 : j + ln + 3], "big")
        if buf[j + total - 2 : j + total] != TRAILER or not crc_ok:
            pos = j + 2
            garbage += 2
            continue
        proto = buf[j + 3]
        serial = int.from_bytes(buf[j + ln - 1 : j + ln + 1], "big")
        payload = bytes(buf[j + 4 : j + ln - 1])
        out.append((proto, serial, payload))
        pos = j + total


def utc_now_s() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)


# ----------------------------------------------------------------------------
# MySQL helpers
# ----------------------------------------------------------------------------

def db_connect(args):
    if pymysql is None:
        raise SystemExit("pymysql missing: pip install pymysql")
    return pymysql.connect(
        host=args.mysql_host, port=args.mysql_port, user=args.mysql_user,
        password=args.mysql_password, database=args.mysql_db,
        autocommit=True, charset="utf8mb4")


def db_clean(args):
    """Wipe both tables. NEVER called automatically: a long-lived server
    legitimately caches device ids, and truncating tables underneath it
    would invalidate that cache. All verification is instead scoped to the
    randomly-generated IMEIs of the current run. Use --truncate to invoke
    this explicitly (then restart the server under test)."""
    conn = db_connect(args)
    with conn.cursor() as c:
        c.execute("SET FOREIGN_KEY_CHECKS=0")
        c.execute("TRUNCATE TABLE positions")
        c.execute("TRUNCATE TABLE devices")
        c.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.close()


def _chunks(seq, n=1000):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _scoped_count(conn, imeis):
    total = 0
    with conn.cursor() as c:
        for chunk in _chunks(imeis):
            fmt = ",".join(["%s"] * len(chunk))
            c.execute(f"SELECT COUNT(*) FROM positions p JOIN devices d"
                      f" ON p.device_id = d.id WHERE d.imei IN ({fmt})",
                      chunk)
            total += c.fetchone()[0]
    return total


def db_wait_rows(args, expected: int, timeout: float, imeis):
    """Poll until this run's position rows reach expected (or stop growing).
    Returns (final_count, seconds_waited)."""
    conn = db_connect(args)
    t0 = time.time()
    last = -1
    last_change = t0
    while True:
        cnt = _scoped_count(conn, imeis)
        now = time.time()
        if cnt != last:
            last, last_change = cnt, now
        if cnt >= expected:
            conn.close()
            return cnt, now - t0
        if now - t0 > timeout or (cnt == last and now - last_change > 12):
            conn.close()
            return cnt, now - t0
        time.sleep(1.0)


def db_verify(args, ground_truth: dict, expected_devices: set,
              max_detail: int = 5):
    """Compare stored rows against ground truth, scoped to this run's IMEIs.
    ground_truth: {(imei, serial): (fix_time, valid, lat, lon, speed, course, sats)}
    Returns dict with counts + ok flag; prints detail for first mismatches."""
    conn = db_connect(args)
    res = {"expected": len(ground_truth), "rows": 0, "missing": 0,
           "extra": 0, "mismatch": 0, "dupes": 0, "devices_ok": False,
           "ok": False}
    details = []
    seen = set()
    with conn.cursor(pymysql.cursors.SSCursor) as c:
        for chunk in _chunks(expected_devices):
            fmt = ",".join(["%s"] * len(chunk))
            c.execute(f"SELECT d.imei, p.serial, p.fix_time, p.valid,"
                      f" p.latitude, p.longitude, p.speed, p.course,"
                      f" p.satellites FROM positions p JOIN devices d"
                      f" ON p.device_id = d.id WHERE d.imei IN ({fmt})",
                      chunk)
            for imei, serial, ft, valid, lat, lon, speed, course, sats in c:
                res["rows"] += 1
                key = (imei, serial)
                if key in seen:
                    res["dupes"] += 1
                    continue
                seen.add(key)
                exp = ground_truth.get(key)
                if exp is None:
                    res["extra"] += 1
                    if len(details) < max_detail:
                        details.append(f"  EXTRA row {key}")
                    continue
                eft, evalid, elat, elon, espeed, ecourse, esats = exp
                bad = []
                if ft != eft:
                    bad.append(f"fix_time {ft!r} != {eft!r}")
                if int(valid) != evalid:
                    bad.append(f"valid {valid} != {evalid}")
                if abs(lat - elat) > 5e-7:
                    bad.append(f"latitude {lat!r} != {elat!r}")
                if abs(lon - elon) > 5e-7:
                    bad.append(f"longitude {lon!r} != {elon!r}")
                if speed != espeed:
                    bad.append(f"speed {speed} != {espeed}")
                if course != ecourse:
                    bad.append(f"course {course} != {ecourse}")
                if sats != esats:
                    bad.append(f"satellites {sats} != {esats}")
                if bad:
                    res["mismatch"] += 1
                    if len(details) < max_detail:
                        details.append(f"  BAD {key}: " + "; ".join(bad))
    res["missing"] = len(ground_truth) - len(seen.intersection(ground_truth))
    db_devs = set()
    with conn.cursor() as c:
        for chunk in _chunks(expected_devices):
            fmt = ",".join(["%s"] * len(chunk))
            c.execute(f"SELECT imei FROM devices WHERE imei IN ({fmt})", chunk)
            db_devs.update(r[0] for r in c.fetchall())
    conn.close()
    res["devices_ok"] = db_devs == set(expected_devices)
    if not res["devices_ok"] and len(details) < max_detail:
        details.append(f"  devices table missing"
                       f" {len(set(expected_devices) - db_devs)} of this"
                       f" run's {len(expected_devices)} IMEIs")
    res["ok"] = (res["missing"] == 0 and res["extra"] == 0 and
                 res["mismatch"] == 0 and res["dupes"] == 0 and
                 res["devices_ok"])
    for d in details:
        print(d)
    return res


class ConnSampler(threading.Thread):
    """Samples MySQL Threads_connected while the load runs. The tester holds
    no DB connections during the send window, so (peak - 1 for the sampler
    itself) = connections held by the server under test. Enforces the
    production-style pool budget (--max-db-conns)."""

    def __init__(self, args):
        super().__init__(daemon=True, name="conn-sampler")
        self.args = args
        self.stop_evt = threading.Event()
        self.max_seen = 0
        self.error = None

    def run(self):
        try:
            conn = db_connect(self.args)
            with conn.cursor() as c:
                while not self.stop_evt.wait(2.0):
                    c.execute("SHOW STATUS LIKE 'Threads_connected'")
                    row = c.fetchone()
                    if row:
                        self.max_seen = max(self.max_seen, int(row[1]))
            conn.close()
        except Exception as e:  # noqa: BLE001 - report any sampler failure
            self.error = e

    def stop(self):
        self.stop_evt.set()
        self.join(timeout=5)
        return max(0, self.max_seen - 1)  # exclude the sampler's own conn


def db_schema_check(args):
    """Loose structural check: tables, engine, unique key, column names."""
    problems = []
    conn = db_connect(args)
    with conn.cursor() as c:
        c.execute("SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES"
                  " WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN"
                  " ('devices','positions')", (args.mysql_db,))
        info = {r[0]: r[1] for r in c.fetchall()}
        for t in ("devices", "positions"):
            if t not in info:
                problems.append(f"table '{t}' missing")
            elif (info[t] or "").upper() != "INNODB":
                problems.append(f"table '{t}' engine {info[t]}, must be InnoDB")
        if "positions" in info:
            c.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS"
                      " WHERE TABLE_SCHEMA=%s AND TABLE_NAME='positions'",
                      (args.mysql_db,))
            cols = {r[0].lower() for r in c.fetchall()}
            need = {"device_id", "serial", "fix_time", "valid", "latitude",
                    "longitude", "speed", "course", "satellites"}
            miss = need - cols
            if miss:
                problems.append(f"positions missing columns: {sorted(miss)}")
            c.execute("SELECT COUNT(*) FROM information_schema.STATISTICS"
                      " WHERE TABLE_SCHEMA=%s AND TABLE_NAME='positions'"
                      " AND NON_UNIQUE=0 AND INDEX_NAME<>'PRIMARY'",
                      (args.mysql_db,))
            if c.fetchone()[0] == 0:
                problems.append("positions has no unique key on"
                                " (device_id, serial)")
    conn.close()
    return problems


# ----------------------------------------------------------------------------
# scripted test connection (basic / stream modes)
# ----------------------------------------------------------------------------

class TestConn:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.reader = self.writer = None
        self.buf = bytearray()
        self.garbage = 0

    async def open(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port)
        sock = self.writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    async def send(self, data: bytes):
        self.writer.write(data)
        await self.writer.drain()

    async def expect(self, n: int, timeout: float = 5.0):
        """Collect n parsed frames within timeout. Returns list (may be short)."""
        frames = []
        deadline = time.monotonic() + timeout
        while len(frames) < n:
            left = deadline - time.monotonic()
            if left <= 0:
                break
            try:
                chunk = await asyncio.wait_for(self.reader.read(4096), left)
            except asyncio.TimeoutError:
                break
            if not chunk:
                break
            self.buf += chunk
            got, g = extract_frames(self.buf)
            self.garbage += g
            frames.extend(got)
        return frames

    async def expect_silence(self, seconds: float):
        """True if no valid frame arrives within the window."""
        frames = await self.expect(1, seconds)
        return len(frames) == 0

    def abort(self):
        if self.writer is not None:
            self.writer.transport.abort()

    async def close(self):
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except (ConnectionError, OSError):
                pass


class Check:
    """Collects named pass/fail results with point weights."""

    def __init__(self, title):
        self.title = title
        self.items = []

    def add(self, name, points, ok, detail=""):
        self.items.append((name, points, bool(ok), detail))
        mark = "PASS" if ok else "FAIL"
        line = f"  [{mark}] {name} ({points} pts)"
        if detail and not ok:
            line += f" -- {detail}"
        print(line)

    def score(self):
        got = sum(p for _, p, ok, _ in self.items if ok)
        total = sum(p for _, p, _, _ in self.items)
        return got, total

    def summary(self):
        got, total = self.score()
        print(f"  => {self.title}: {got}/{total} pts")
        return got, total


def rand_imei(rng):
    return "86" + "".join(rng.choice("0123456789") for _ in range(13))


# ----------------------------------------------------------------------------
# basic mode
# ----------------------------------------------------------------------------

async def mode_basic(args):
    print("\n=== BASIC (functional correctness) ===")
    rng = random.Random(f"{args.seed}/basic")
    ck = Check("BASIC")
    use_db = not args.no_db
    if use_db:
        problems = db_schema_check(args)
        ck.add("schema matches spec", 5, not problems, "; ".join(problems))
    gt = {}
    devices = set()

    # 1. login handshake
    imei_a = rand_imei(rng)
    devices.add(imei_a)
    serial = rng.randint(1, 400)
    conn = TestConn(args.host, args.port)
    ok_login = False
    try:
        await conn.open()
        await conn.send(build_login(imei_a, serial))
        frames = await conn.expect(1, 5)
        ok_login = frames == [(PROTO_LOGIN, serial, b"")]
    except OSError as e:
        ck.add("login handshake", 5, False, f"connect failed: {e}")
        print("  cannot reach server, aborting basic mode")
        return ck.summary()
    ck.add("login handshake", 5, ok_login,
           f"expected login ack serial={serial}, got {frames!r}")

    # 2. positions with edge-case field values, one frame per send
    base = utc_now_s()
    cases = [
        # lat, lon, speed, course, sats, valid
        (28.613939, 77.209023, 62, 145, 9, True),     # Delhi, NE
        (-33.868820, 151.209290, 0, 0, 4, True),      # Sydney, SE, min speed
        (34.603722 * -1, -58.381592, 255, 359, 15, True),  # Buenos Aires, SW, maxima
        (51.507351, -0.127758, 118, 273, 12, True),   # London, NW
        (28.700001, 77.100001, 40, 90, 7, False),     # invalid fix still stored
    ]
    acked = 0
    for i, (lat, lon, speed, course, sats, valid) in enumerate(cases):
        serial += 1
        dt = base.replace(second=(base.second + i) % 60)
        frame, fields = encode_position(serial, dt, lat, lon, speed, course,
                                        sats, valid)
        gt[(imei_a, serial)] = fields
        await conn.send(frame)
        frames = await conn.expect(1, 3)
        if frames == [(PROTO_POSITION, serial, b"")]:
            acked += 1
    ck.add("positions acked (echo serial)", 5, acked == len(cases),
           f"{acked}/{len(cases)} acks")
    await conn.close()

    # 3. re-login same IMEI on a new connection (reconnect flow)
    conn2 = TestConn(args.host, args.port)
    await conn2.open()
    serial += 1
    await conn2.send(build_login(imei_a, serial))
    f2 = await conn2.expect(1, 5)
    relog = f2 == [(PROTO_LOGIN, serial, b"")]
    serial += 1
    frame, fields = encode_position(serial, base, 28.62, 77.22, 30, 180, 8, True)
    gt[(imei_a, serial)] = fields
    await conn2.send(frame)
    f3 = await conn2.expect(1, 3)
    relog = relog and f3 == [(PROTO_POSITION, serial, b"")]
    await conn2.close()
    ck.add("reconnect: re-login then position", 2, relog)

    # 4. second device
    imei_b = rand_imei(rng)
    devices.add(imei_b)
    conn3 = TestConn(args.host, args.port)
    await conn3.open()
    s_b = rng.randint(1, 400)
    await conn3.send(build_login(imei_b, s_b))
    await conn3.expect(1, 5)
    s_b += 1
    frame, fields = encode_position(s_b, base, 19.076090, 72.877426, 55, 220,
                                    11, True)
    gt[(imei_b, s_b)] = fields
    await conn3.send(frame)
    await conn3.expect(1, 3)
    await conn3.close()

    # 5. storage
    if use_db:
        cnt, waited = db_wait_rows(args, len(gt), args.db_wait, devices)
        res = db_verify(args, gt, devices)
        ck.add("rows stored exactly (values, dedup, devices)", 8, res["ok"],
               f"rows={res['rows']} expected={res['expected']}"
               f" missing={res['missing']} extra={res['extra']}"
               f" mismatch={res['mismatch']} (waited {waited:.0f}s)")
    else:
        ck.add("rows stored exactly (values, dedup, devices)", 8, False,
               "skipped (--no-db)")
    return ck.summary()


# ----------------------------------------------------------------------------
# stream mode
# ----------------------------------------------------------------------------

async def mode_stream(args):
    print("\n=== STREAM (TCP robustness) ===")
    rng = random.Random(f"{args.seed}/stream")
    ck = Check("STREAM")
    use_db = not args.no_db
    gt = {}
    devices = set()
    base = utc_now_s()

    # 1. coalesced: login + 5 positions in ONE send()
    imei = rand_imei(rng)
    devices.add(imei)
    s = 10
    blob = build_login(imei, s)
    want = [(PROTO_LOGIN, s, b"")]
    for i in range(5):
        s += 1
        frame, fields = encode_position(s, base, 12.9716 + i * 0.001, 77.5946,
                                        20 + i, 45, 8, True)
        gt[(imei, s)] = fields
        blob += frame
        want.append((PROTO_POSITION, s, b""))
    conn = TestConn(args.host, args.port)
    await conn.open()
    await conn.send(blob)
    frames = await conn.expect(len(want), 5)
    ck.add("coalesced frames (6 in one segment)", 4,
           sorted(frames) == sorted(want), f"got {len(frames)}/{len(want)} acks")
    await conn.close()

    # 2. fragmented: bytes dribbled 1-7 at a time
    imei = rand_imei(rng)
    devices.add(imei)
    s = 700
    blob = build_login(imei, s)
    want = [(PROTO_LOGIN, s, b"")]
    for i in range(3):
        s += 1
        frame, fields = encode_position(s, base, 13.0827, 80.2707 + i * 0.001,
                                        60, 270, 10, True)
        gt[(imei, s)] = fields
        blob += frame
        want.append((PROTO_POSITION, s, b""))
    conn = TestConn(args.host, args.port)
    await conn.open()
    i = 0
    while i < len(blob):
        n = rng.randint(1, 7)
        await conn.send(blob[i:i + n])
        i += n
        await asyncio.sleep(0.002)
    frames = await conn.expect(len(want), 6)
    ck.add("fragmented frames (1-7 byte chunks)", 4,
           sorted(frames) == sorted(want), f"got {len(frames)}/{len(want)} acks")
    await conn.close()

    # 3. corrupted CRC frame must be skipped, neighbours still processed
    imei = rand_imei(rng)
    devices.add(imei)
    s = 40
    conn = TestConn(args.host, args.port)
    await conn.open()
    await conn.send(build_login(imei, s))
    await conn.expect(1, 5)
    good1, f1 = encode_position(s + 1, base, 22.5726, 88.3639, 33, 120, 9, True)
    bad, _ = encode_position(s + 2, base, 22.5730, 88.3640, 34, 121, 9, True)
    good2, f2 = encode_position(s + 3, base, 22.5734, 88.3641, 35, 122, 9, True)
    gt[(imei, s + 1)] = f1
    gt[(imei, s + 3)] = f2
    await conn.send(good1 + corrupt_frame(bad) + good2)
    frames = await conn.expect(3, 4)  # ask for 3, expect only 2
    ok = sorted(frames) == sorted(
        [(PROTO_POSITION, s + 1, b""), (PROTO_POSITION, s + 3, b"")])
    ck.add("bad CRC dropped silently, stream continues", 4, ok,
           f"acks={frames!r}")
    await conn.close()

    # 4. duplicate serial: ack again, store once
    imei = rand_imei(rng)
    devices.add(imei)
    s = 55
    conn = TestConn(args.host, args.port)
    await conn.open()
    await conn.send(build_login(imei, s))
    await conn.expect(1, 5)
    frame, fields = encode_position(s + 1, base, 17.3850, 78.4867, 44, 200, 13,
                                    True)
    gt[(imei, s + 1)] = fields
    await conn.send(frame)
    a1 = await conn.expect(1, 3)
    await conn.send(frame)  # retransmission of the same packet
    a2 = await conn.expect(1, 3)
    ok = (a1 == [(PROTO_POSITION, s + 1, b"")]
          and a2 == [(PROTO_POSITION, s + 1, b"")])
    ck.add("duplicate serial re-acked", 4, ok, f"a1={a1!r} a2={a2!r}")
    await conn.close()

    # 5. position before login: silently ignored, connection stays usable
    imei = rand_imei(rng)
    devices.add(imei)
    conn = TestConn(args.host, args.port)
    await conn.open()
    orphan, _ = encode_position(999, base, 10.0, 76.0, 10, 10, 5, True)
    await conn.send(orphan)
    silent = await conn.expect_silence(2.0)
    await conn.send(build_login(imei, 60))
    f_login = await conn.expect(1, 5)
    frame, fields = encode_position(61, base, 10.1, 76.1, 11, 11, 6, True)
    gt[(imei, 61)] = fields
    await conn.send(frame)
    f_pos = await conn.expect(1, 3)
    ok = (silent and f_login == [(PROTO_LOGIN, 60, b"")]
          and f_pos == [(PROTO_POSITION, 61, b"")])
    ck.add("position before login ignored (no ack, no row)", 2, ok,
           f"silent={silent} login={f_login!r} pos={f_pos!r}")
    await conn.close()

    # 6. abrupt disconnect + reconnect + re-login
    imei = rand_imei(rng)
    devices.add(imei)
    conn = TestConn(args.host, args.port)
    await conn.open()
    await conn.send(build_login(imei, 70))
    await conn.expect(1, 5)
    frame, fields = encode_position(71, base, 23.0225, 72.5714, 66, 310, 9, True)
    gt[(imei, 71)] = fields
    await conn.send(frame)
    await conn.expect(1, 3)
    conn.abort()  # simulate network drop (RST, no FIN)
    await asyncio.sleep(0.3)
    conn = TestConn(args.host, args.port)
    await conn.open()
    await conn.send(build_login(imei, 72))
    fl = await conn.expect(1, 5)
    frame, fields = encode_position(73, base, 23.0230, 72.5720, 67, 311, 9, True)
    gt[(imei, 73)] = fields
    await conn.send(frame)
    fp = await conn.expect(1, 3)
    ok = fl == [(PROTO_LOGIN, 72, b"")] and fp == [(PROTO_POSITION, 73, b"")]
    ck.add("abrupt drop -> reconnect -> re-login works", 2, ok)
    await conn.close()

    if use_db:
        cnt, waited = db_wait_rows(args, len(gt), args.db_wait, devices)
        res = db_verify(args, gt, devices)
        detail = (f"rows={res['rows']} expected={res['expected']}"
                  f" missing={res['missing']} extra={res['extra']}"
                  f" mismatch={res['mismatch']} dupes={res['dupes']}")
        # storage correctness under stream edge cases is folded into the
        # individual checks above; a wrong row count here fails all of them
        if not res["ok"]:
            print(f"  !! storage mismatch after stream tests: {detail}")
            ck.add("stream storage exact (corrupt/dup rows excluded)", 0,
                   False, detail)
        else:
            print(f"  storage verified: {detail}")
    return ck.summary()


# ----------------------------------------------------------------------------
# load / ramp / traccar modes
# ----------------------------------------------------------------------------

class LoadCtx:
    def __init__(self):
        self.ground_truth = {}
        self.latencies = []
        self.measure_start = 0.0  # perf_counter; acks for packets sent
        # before this moment are excluded from latency percentiles
        # (JIT/cache warmup), but still count for loss + storage.
        self.start_evt = asyncio.Event()
        self.stop_evt = asyncio.Event()
        self.kill_evt = asyncio.Event()
        self.connected = 0
        self.conn_fail = 0
        self.conn_drop = 0
        self.login_sent = 0
        self.login_acked = 0
        self.login_timeout = 0
        self.pos_sent = 0
        self.pos_acked = 0
        self.retransmits = 0
        self.churns = 0
        self.garbage = 0
        self.unexpected = 0


class SimDevice:
    __slots__ = ("imei", "rng", "serial", "lat", "lon", "pending", "send_t",
                 "login_ok", "expect_ack")

    def __init__(self, imei, rng, expect_ack=True):
        self.imei = imei
        self.rng = rng
        self.serial = rng.randint(1, 500)
        self.lat = rng.uniform(8.0, 30.0)
        self.lon = rng.uniform(70.0, 88.0)
        self.pending = {}
        self.send_t = {}
        self.login_ok = None  # asyncio.Event, created per run
        self.expect_ack = expect_ack

    def take_serial(self):
        s = self.serial
        self.serial = (self.serial + 1) & 0xFFFF
        return s

    def build_position(self, ctx):
        rng = self.rng
        self.lat = min(35.0, max(6.0, self.lat + rng.uniform(-9e-4, 9e-4)))
        self.lon = min(95.0, max(68.0, self.lon + rng.uniform(-9e-4, 9e-4)))
        s = self.take_serial()
        frame, fields = encode_position(
            s, utc_now_s(), self.lat, self.lon, rng.randint(0, 120),
            rng.randint(0, 359), rng.randint(4, 14),
            rng.random() < 0.98, mnc=rng.randint(1, 99),
            lac=rng.randint(1, 0xFFFF), cid=rng.randint(1, 0xFFFFFF))
        ctx.ground_truth[(self.imei, s)] = fields
        if self.expect_ack:
            self.pending[s] = frame
            self.send_t[s] = time.perf_counter()
        ctx.pos_sent += 1
        return frame


async def device_reader(dev, reader, ctx):
    buf = bytearray()
    try:
        while True:
            chunk = await reader.read(8192)
            if not chunk:
                return
            buf += chunk
            frames, garbage = extract_frames(buf)
            ctx.garbage += garbage
            t = time.perf_counter()
            for proto, serial, _payload in frames:
                if proto == PROTO_LOGIN:
                    dev.login_ok.set()
                elif proto == PROTO_POSITION:
                    if dev.pending.pop(serial, None) is not None:
                        ctx.pos_acked += 1
                        st = dev.send_t.pop(serial, None)
                        if st is not None and st >= ctx.measure_start:
                            ctx.latencies.append(t - st)
                else:
                    ctx.unexpected += 1
    except (asyncio.CancelledError, ConnectionError, OSError):
        return


async def device_main(dev, cfg, ctx):
    loop = asyncio.get_running_loop()
    backoff = 0.5
    while not ctx.kill_evt.is_set():
        if ctx.stop_evt.is_set() and not dev.pending:
            return
        try:
            reader, writer = await asyncio.open_connection(
                cfg["host"], cfg["port"], limit=1 << 16)
        except OSError:
            ctx.conn_fail += 1
            await asyncio.sleep(backoff + dev.rng.random() * 0.5)
            backoff = min(backoff * 2, 5.0)
            continue
        backoff = 0.5
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        ctx.connected += 1
        dev.login_ok = asyncio.Event()
        rtask = asyncio.create_task(device_reader(dev, reader, ctx))
        aborted = False
        try:
            writer.write(build_login(dev.imei, dev.take_serial()))
            await writer.drain()
            ctx.login_sent += 1
            try:
                await asyncio.wait_for(dev.login_ok.wait(), 10)
            except asyncio.TimeoutError:
                ctx.login_timeout += 1
                aborted = True
                writer.transport.abort()
                continue
            ctx.login_acked += 1
            if dev.pending:  # retransmit unacked positions after reconnect
                now = time.perf_counter()
                for s in dev.send_t:
                    dev.send_t[s] = now
                writer.write(b"".join(dev.pending.values()))
                await writer.drain()
                ctx.retransmits += len(dev.pending)
            if not ctx.stop_evt.is_set():
                if not ctx.start_evt.is_set():
                    await ctx.start_evt.wait()
                interval = cfg["interval"]
                next_t = loop.time() + dev.rng.random() * interval
                churn_me = False
                while not ctx.stop_evt.is_set():
                    now = loop.time()
                    if now < next_t:
                        await asyncio.sleep(min(next_t - now, 0.5))
                        continue
                    writer.write(dev.build_position(ctx))
                    await writer.drain()
                    next_t += interval
                    if cfg["churn"] > 0 and dev.rng.random() < cfg["churn"] * interval:
                        churn_me = True
                        break
                if churn_me and not ctx.stop_evt.is_set():
                    ctx.churns += 1
                    aborted = True
                    writer.transport.abort()
                    continue
            # drain: wait for outstanding acks (or kill)
            while dev.pending and not ctx.kill_evt.is_set():
                await asyncio.sleep(0.2)
        except (ConnectionError, OSError):
            ctx.conn_drop += 1
        finally:
            ctx.connected -= 1
            rtask.cancel()
            if not aborted:
                try:
                    writer.close()
                except (ConnectionError, OSError):
                    pass
        if ctx.stop_evt.is_set() and not dev.pending:
            return


async def progress_task(ctx, tag, nshards):
    t0 = time.monotonic()
    last_sent = 0
    scale = f" (x{nshards} shards)" if nshards > 1 else ""
    while not ctx.kill_evt.is_set():
        await asyncio.sleep(5)
        t = time.monotonic() - t0
        rate = (ctx.pos_sent - last_sent) / 5.0
        last_sent = ctx.pos_sent
        print(f"{tag} [t+{t:5.0f}s] conns={ctx.connected} sent={ctx.pos_sent}"
              f" acked={ctx.pos_acked} rate={rate:,.0f}/s"
              f" churns={ctx.churns} conn_fail={ctx.conn_fail}{scale}",
              flush=True)


def shard_slice(total, nshards, shard):
    base, rem = divmod(total, nshards)
    return base + (1 if shard < rem else 0)


async def run_load(args, devices_n, duration, expect_ack=True,
                   shard=0, nshards=1, quiet=False):
    """Core load engine for one shard. Returns metrics dict (raw latencies
    included; percentiles are computed after merging shards)."""
    cfg = {"host": args.host, "port": args.port, "interval": args.interval,
           "churn": args.churn}
    rng = random.Random(f"{args.seed}/load/{devices_n}/{shard}")
    my_n = shard_slice(devices_n, nshards, shard)
    login_rate = max(1, args.login_rate // nshards)
    ctx = LoadCtx()
    imeis = []
    seen = set()
    while len(imeis) < my_n:
        imei = rand_imei(rng)
        if imei not in seen:
            seen.add(imei)
            imeis.append(imei)
    devs = [SimDevice(im, random.Random(rng.random()), expect_ack=expect_ack)
            for im in imeis]

    tag = f"  [shard {shard}]" if nshards > 1 else " "
    tasks = []
    ramp_t0 = time.monotonic()
    for i in range(0, len(devs), login_rate):
        chunk = devs[i:i + login_rate]
        tasks += [asyncio.create_task(device_main(d, cfg, ctx)) for d in chunk]
        await asyncio.sleep(1.0)
    # wait for logins to settle
    settle_deadline = time.monotonic() + 15
    while ctx.login_acked < my_n and time.monotonic() < settle_deadline:
        await asyncio.sleep(0.5)
    ramp_s = time.monotonic() - ramp_t0
    if not quiet:
        print(f"{tag} ramp done in {ramp_s:.1f}s: {ctx.login_acked}/{my_n}"
              f" devices logged in (timeouts={ctx.login_timeout},"
              f" conn_fail={ctx.conn_fail})", flush=True)

    prog = None
    if not quiet:
        prog = asyncio.create_task(progress_task(ctx, tag, nshards))
    t_start = time.monotonic()
    ctx.measure_start = time.perf_counter() + args.warmup
    ctx.start_evt.set()
    await asyncio.sleep(duration)
    ctx.stop_evt.set()
    send_s = time.monotonic() - t_start

    # drain phase: let acks arrive / retransmits finish
    drain_deadline = time.monotonic() + args.drain
    while time.monotonic() < drain_deadline:
        pend = sum(len(d.pending) for d in devs)
        if pend == 0:
            break
        await asyncio.sleep(0.5)
    drain_s = time.monotonic() - t_start - send_s
    ctx.kill_evt.set()
    if prog is not None:
        prog.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    unacked = sum(len(d.pending) for d in devs)
    m = {
        "devices": my_n,
        "login_sent": ctx.login_sent, "login_acked": ctx.login_acked,
        "login_timeout": ctx.login_timeout,
        "sent": ctx.pos_sent, "acked": ctx.pos_acked, "unacked": unacked,
        "unique_sent": len(ctx.ground_truth),
        "send_s": send_s, "drain_s": drain_s, "churns": ctx.churns,
        "retransmits": ctx.retransmits, "conn_fail": ctx.conn_fail,
        "conn_drop": ctx.conn_drop, "garbage": ctx.garbage,
        "latencies": ctx.latencies,
        "ground_truth": ctx.ground_truth,
        "imeis": {d.imei for d in devs},
    }
    return m


def finalize_metrics(m):
    lat = sorted(m.pop("latencies"))

    def pct(p):
        return lat[min(len(lat) - 1, int(len(lat) * p))] * 1000 if lat else 0

    m["rate"] = m["sent"] / m["send_s"] if m["send_s"] else 0
    m["p50"], m["p95"], m["p99"] = pct(0.50), pct(0.95), pct(0.99)
    m["max_ms"] = lat[-1] * 1000 if lat else 0
    return m


def _shard_worker(args_dict, devices_n, duration, expect_ack, shard, nshards,
                  pipe):
    args = argparse.Namespace(**args_dict)
    m = asyncio.run(run_load(args, devices_n, duration, expect_ack,
                             shard=shard, nshards=nshards,
                             quiet=(shard != 0)))
    m["ground_truth"] = list(m["ground_truth"].items())
    m["imeis"] = list(m["imeis"])
    pipe.send(m)
    pipe.close()


async def run_load_sharded(args, devices_n, duration, expect_ack=True):
    """Fan the load engine out over --shards worker processes so the tester
    itself never becomes the bottleneck, then merge metrics."""
    nshards = args.shards
    print(f"  devices={devices_n} interval={args.interval}s"
          f" duration={duration}s churn={args.churn * 100:.1f}%/s"
          f" target={devices_n / args.interval:,.0f} pos/s"
          f" shards={nshards}")
    if nshards <= 1:
        return finalize_metrics(
            await run_load(args, devices_n, duration, expect_ack))
    loop = asyncio.get_running_loop()
    procs = []
    for s in range(nshards):
        rx, tx = multiprocessing.Pipe(False)
        p = multiprocessing.Process(
            target=_shard_worker,
            args=(vars(args), devices_n, duration, expect_ack, s, nshards, tx),
            daemon=True)
        p.start()
        tx.close()
        procs.append((p, rx))
    parts = []
    for p, rx in procs:
        parts.append(await loop.run_in_executor(None, rx.recv))
        p.join()
    m = {k: sum(x[k] for x in parts) for k in
         ("devices", "login_sent", "login_acked", "login_timeout", "sent",
          "acked", "unacked", "unique_sent", "churns", "retransmits",
          "conn_fail", "conn_drop", "garbage")}
    m["send_s"] = max(x["send_s"] for x in parts)
    m["drain_s"] = max(x["drain_s"] for x in parts)
    m["latencies"] = [v for x in parts for v in x["latencies"]]
    gt = {}
    imeis = set()
    for x in parts:
        gt.update(dict(x["ground_truth"]))
        imeis.update(x["imeis"])
    m["ground_truth"] = gt
    m["imeis"] = imeis
    return finalize_metrics(m)


def print_load_metrics(m):
    print(f"  sent={m['sent']:,} unique={m['unique_sent']:,}"
          f" acked={m['acked']:,} unacked={m['unacked']:,}"
          f" retransmits={m['retransmits']:,}")
    print(f"  send window={m['send_s']:.1f}s"
          f" achieved rate={m['rate']:,.0f} pos/s"
          f" drain={m['drain_s']:.1f}s churns={m['churns']}")
    print(f"  ack latency ms: p50={m['p50']:.0f} p95={m['p95']:.0f}"
          f" p99={m['p99']:.0f} max={m['max_ms']:.0f}")
    print(f"  logins: sent={m['login_sent']} acked={m['login_acked']}"
          f" timeouts={m['login_timeout']} conn_fail={m['conn_fail']}"
          f" conn_drop={m['conn_drop']}")
    if m["garbage"]:
        print(f"  !! server sent {m['garbage']} bytes of invalid/garbled"
              f" response data")


async def mode_load(args):
    print("\n=== LOAD (concurrency + throughput + integrity) ===")
    ck = Check("LOAD")
    use_db = not args.no_db
    sampler = None
    if use_db:
        sampler = ConnSampler(args)
        sampler.start()
    m = await run_load_sharded(args, args.devices, args.duration)
    db_conns = sampler.stop() if sampler else 0
    print_load_metrics(m)
    if sampler:
        print(f"  peak MySQL connections held by server: {db_conns}"
              f" (budget {args.max_db_conns})")

    conn_budget = max(10, int(0.02 * (m["devices"] + m["churns"])))
    ck.add("all devices logged in (incl. reconnects)", 5,
           m["login_acked"] == m["login_sent"] and m["login_timeout"] == 0
           and m["conn_fail"] <= conn_budget,
           f"acked={m['login_acked']}/{m['login_sent']}"
           f" timeouts={m['login_timeout']} conn_fail={m['conn_fail']}"
           f" (budget {conn_budget})")
    ck.add("zero position loss (every packet acked)", 10,
           m["unacked"] == 0 and m["garbage"] == 0,
           f"unacked={m['unacked']} garbage={m['garbage']}")
    ck.add(f"p99 ack latency <= {args.p99_ms} ms", 10,
           0 < len(m["ground_truth"]) and m["p99"] <= args.p99_ms,
           f"p99={m['p99']:.0f} ms")
    if use_db:
        if sampler.error is not None:
            ck.add(f"<= {args.max_db_conns} MySQL connections under load", 5,
                   False, f"sampler failed: {sampler.error}")
        else:
            ck.add(f"<= {args.max_db_conns} MySQL connections under load", 5,
                   0 < db_conns <= args.max_db_conns,
                   f"peak={db_conns}")
    else:
        ck.add(f"<= {args.max_db_conns} MySQL connections under load", 5,
               False, "skipped (--no-db)")
    if use_db:
        cnt, waited = db_wait_rows(args, m["unique_sent"], args.db_wait,
                                   m["imeis"])
        print(f"  db row count {cnt:,}/{m['unique_sent']:,}"
              f" after {waited:.0f}s; verifying values...")
        res = db_verify(args, m["ground_truth"], m["imeis"])
        ck.add(f"storage complete + exact within {args.db_wait:.0f}s", 15,
               res["ok"],
               f"rows={res['rows']:,} missing={res['missing']:,}"
               f" extra={res['extra']:,} mismatch={res['mismatch']:,}"
               f" dupes={res['dupes']:,} devices_ok={res['devices_ok']}")
    else:
        ck.add("storage complete + exact", 15, False, "skipped (--no-db)")
    return ck.summary()


async def mode_ramp(args):
    print("\n=== RAMP (max concurrent devices ladder) ===")
    ladder = [int(x) for x in args.ladder.split(",")]
    use_db = not args.no_db
    best = 0
    results = []
    for n in ladder:
        print(f"\n  --- step: {n} devices"
              f" ({n / args.interval:,.0f} pos/s target) ---")
        sampler = None
        if use_db:
            sampler = ConnSampler(args)
            sampler.start()
        m = await run_load_sharded(args, n, args.ramp_duration)
        db_conns = sampler.stop() if sampler else 0
        print_load_metrics(m)
        if sampler:
            print(f"  peak MySQL connections held by server: {db_conns}"
                  f" (budget {args.max_db_conns})")
        conn_budget = max(10, int(0.02 * (m["devices"] + m["churns"])))
        ok = (m["unacked"] == 0 and m["login_timeout"] == 0
              and m["login_acked"] == m["login_sent"]
              and m["conn_fail"] <= conn_budget and m["p99"] <= args.p99_ms
              and (not use_db or (sampler.error is None
                                  and 0 < db_conns <= args.max_db_conns)))
        if ok and use_db:
            cnt, waited = db_wait_rows(args, m["unique_sent"], args.db_wait,
                                       m["imeis"])
            ok = cnt == m["unique_sent"]
            print(f"  db count {cnt:,}/{m['unique_sent']:,} ({waited:.0f}s)")
        results.append((n, ok, m["rate"], m["p99"]))
        print(f"  step {n}: {'PASS' if ok else 'FAIL'}")
        if not ok:
            break
        best = n
        await asyncio.sleep(2)
    print("\n  ladder results:")
    for n, ok, rate, p99 in results:
        print(f"    {n:>7} devices: {'PASS' if ok else 'FAIL':4}"
              f" rate={rate:,.0f}/s p99={p99:.0f}ms")
    print(f"  => MAX CONCURRENT DEVICES: {best}")
    return best, 0


async def mode_traccar(args):
    """Benchmark a real Traccar server: login acks only, verify via
    tc_positions row count. Informational, no pass/fail points."""
    print("\n=== TRACCAR COMPAT (benchmark a real Traccar GT06 port) ===")
    print("  note: stock Traccar does not ack 0x12 GPS packets; loss is"
          " measured via the tc_positions table only")
    if args.register:
        conn = db_connect(args)
        # pre-generate the exact imeis the load shards will use
        seen = []
        for shard in range(args.shards):
            rng = random.Random(f"{args.seed}/load/{args.devices}/{shard}")
            my_n = shard_slice(args.devices, args.shards, shard)
            s = set()
            while len(s) < my_n:
                im = rand_imei(rng)
                if im not in s:
                    s.add(im)
                    seen.append(im)
        with conn.cursor() as c:
            for im in seen:
                try:
                    c.execute("INSERT IGNORE INTO tc_devices (name, uniqueid)"
                              " VALUES (%s, %s)", (im, im))
                except pymysql.err.MySQLError as e:
                    print(f"  register failed ({e}); enable"
                          " 'database.registerUnknown' in traccar.xml instead")
                    break
        conn.close()
        print(f"  registered {len(seen)} devices in tc_devices")
    t_start = datetime.now(timezone.utc).replace(tzinfo=None)
    sampler = None
    if not args.no_db:
        sampler = ConnSampler(args)
        sampler.start()
    m = await run_load_sharded(args, args.devices, args.duration,
                               expect_ack=False)
    db_conns = sampler.stop() if sampler else 0
    print_load_metrics(m)
    if sampler and sampler.error is None:
        print(f"  peak MySQL connections during run: {db_conns}"
              f" (informational; includes Traccar's pool + web API)")
    if not args.no_db:
        time.sleep(min(args.db_wait, 30))
        conn = db_connect(args)
        with conn.cursor() as c:
            fmt = ",".join(["%s"] * len(m["imeis"]))
            c.execute(f"SELECT COUNT(*) FROM tc_positions p JOIN tc_devices d"
                      f" ON p.deviceid = d.id WHERE d.uniqueid IN ({fmt})"
                      f" AND p.servertime >= %s",
                      (*m["imeis"], t_start))
            cnt = c.fetchone()[0]
        conn.close()
        pctv = 100.0 * cnt / m["unique_sent"] if m["unique_sent"] else 0
        print(f"  tc_positions stored: {cnt:,}/{m['unique_sent']:,}"
              f" ({pctv:.1f}%)")
    return 0, 0


# ----------------------------------------------------------------------------
# selftest
# ----------------------------------------------------------------------------

def mode_selftest():
    print("=== SELFTEST (protocol vectors) ===")
    # canonical GT06 spec examples
    login = build_login("123456789012345", 1)
    assert login.hex() == "78780d01012345678901234500018cdd0d0a", login.hex()
    resp = build_frame(PROTO_LOGIN, 1)
    assert resp.hex() == "787805010001d9dc0d0a", resp.hex()
    # crc vectors
    assert crc16_x25(bytes.fromhex("0d0101234567890123450001")) == 0x8CDD
    # position round trip through the stream parser
    dt = datetime(2026, 7, 14, 10, 30, 0)
    frame, fields = encode_position(2, dt, 28.613939, 77.209023, 62, 145, 9,
                                    True)
    assert frame[2] == len(frame) - 5 and frame[3] == 0x12
    buf = bytearray(b"\x00\x99" + frame + frame[:7])  # garbage + frame + partial
    frames, garbage = extract_frames(buf)
    assert frames == [(0x12, 2, frame[4:-6])] and garbage == 2, (frames, garbage)
    assert bytes(buf) == frame[:7]
    # corrupt frame must not parse
    buf = bytearray(corrupt_frame(frame) + frame)
    frames, garbage = extract_frames(buf)
    assert frames == [(0x12, 2, frame[4:-6])] and garbage > 0
    # sign handling: south + west
    f2, fl2 = encode_position(3, dt, -33.86882, -58.381592, 0, 359, 15, False)
    assert fl2[2] < 0 and fl2[3] < 0 and fl2[1] == 0
    # bcd
    assert imei_to_bcd("868120148377198").hex() == "0868120148377198"
    print("  all protocol vectors OK")
    return True


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(description="GT06 hackathon tester")
    p.add_argument("mode", choices=["selftest", "basic", "stream", "load",
                                    "ramp", "traccar", "all"])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5023)
    p.add_argument("--mysql-host", default="127.0.0.1")
    p.add_argument("--mysql-port", type=int, default=3306)
    p.add_argument("--mysql-user", default="hackathon")
    p.add_argument("--mysql-password", default="hackathon123")
    p.add_argument("--mysql-db", default="hackathon")
    p.add_argument("--no-db", action="store_true",
                   help="skip MySQL verification (protocol checks only)")
    p.add_argument("--devices", type=int, default=10000)
    p.add_argument("--interval", type=float, default=1.0,
                   help="seconds between positions per device")
    p.add_argument("--duration", type=float, default=30.0,
                   help="steady-state send window seconds")
    p.add_argument("--churn", type=float, default=0.01,
                   help="fraction of devices that drop+reconnect per second")
    p.add_argument("--login-rate", type=int, default=1000,
                   help="device connections launched per second during ramp")
    p.add_argument("--drain", type=float, default=10.0,
                   help="seconds to wait for outstanding acks after send stops")
    p.add_argument("--warmup", type=float, default=5.0,
                   help="initial seconds excluded from latency percentiles"
                        " (JIT/cache warmup); loss and storage still count")
    p.add_argument("--p99-ms", type=float, default=1000.0)
    p.add_argument("--max-db-conns", type=int, default=25,
                   help="max MySQL connections the server may hold under"
                        " load (production pool budget; sampled via"
                        " Threads_connected)")
    p.add_argument("--db-wait", type=float, default=60.0,
                   help="max seconds for rows to appear in MySQL")
    p.add_argument("--ladder", default="1000,2000,5000,10000,15000,20000")
    p.add_argument("--ramp-duration", type=float, default=15.0)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--shards", type=int, default=0,
                   help="load-generator worker processes (0 = auto: 1 below"
                        " 2000 devices, else up to 8)")
    p.add_argument("--register", action="store_true",
                   help="traccar mode: pre-insert devices into tc_devices")
    p.add_argument("--truncate", action="store_true",
                   help="wipe devices/positions tables before testing"
                        " (restart the server under test afterwards: its"
                        " device-id cache becomes stale)")
    return p


async def amain(args):
    scores = []
    if args.mode in ("basic", "all"):
        scores.append(await mode_basic(args))
    if args.mode in ("stream", "all"):
        scores.append(await mode_stream(args))
    if args.mode in ("load", "all"):
        scores.append(await mode_load(args))
    if args.mode == "ramp":
        await mode_ramp(args)
        return 0
    if args.mode == "traccar":
        await mode_traccar(args)
        return 0
    got = sum(g for g, _ in scores)
    total = sum(t for _, t in scores)
    print(f"\n===== TOTAL SCORE: {got}/{total} =====")
    return 0 if got == total else 1


def main():
    args = build_parser().parse_args()
    if args.seed is None:
        args.seed = random.SystemRandom().randint(1, 10 ** 9)
    if args.shards <= 0:
        args.shards = 1 if args.devices < 2000 else min(
            8, max(2, (os.cpu_count() or 4) // 2))
    print(f"seed={args.seed} target={args.host}:{args.port}"
          f" mysql={args.mysql_host}:{args.mysql_port}/{args.mysql_db}"
          f"{' (db checks OFF)' if args.no_db else ''}")
    if args.mode == "selftest":
        mode_selftest()
        return 0
    mode_selftest()  # always sanity-check the protocol vectors first
    if not args.no_db and pymysql is None:
        raise SystemExit("pymysql missing: pip install pymysql"
                         " (or pass --no-db)")
    if args.truncate and not args.no_db:
        db_clean(args)
        print("tables truncated -- make sure the server under test was"
              " (re)started AFTER this point")
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
