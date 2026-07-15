# GPS Ingest Server Challenge

## The scenario

A vehicle-tracking platform ingests data from tens of thousands of GPS
trackers sitting in trucks across the country. Each tracker opens a plain
TCP connection to the platform and speaks a compact binary protocol (the
same family of protocols real trackers like the GT06 use). The tracker
sends a **login packet** to identify itself, then a **position packet**
every second. If the connection drops — tunnels, dead zones, tower
handovers — the tracker reconnects and **logs in again** before resuming
positions. Every position that the server acknowledges must end up in
MySQL, exactly once, with exactly the right values.

One ingest server must comfortably handle **10,000 concurrently connected
devices sending 10,000 positions per second (100k+ positions every 10
seconds)** — while devices randomly drop and reconnect the whole time.

**Your task: build that server.** Any language, any framework, any
libraries. It must run on your own laptop and store into MySQL. The
official tester connects to your server like a fleet of real devices and
scores it.

## What you must build

A TCP server that:

1. Listens on **port 5023** (`PORT` env var overrides).
2. Speaks the protocol below: parses login and position packets, responds
   ("acks") to each valid one.
3. Stores every accepted position into MySQL using the provided schema
   (`schema.sql`), connecting with these env vars (defaults in brackets):
   `MYSQL_HOST` [127.0.0.1], `MYSQL_PORT` [3306], `MYSQL_USER` [hackathon],
   `MYSQL_PASSWORD` [hackathon123], `MYSQL_DB` [hackathon].
4. Survives the load test: 10,000 devices, 1 position/second each,
   devices randomly disconnecting and re-logging-in continuously.

A position must be visible in MySQL **within 30 seconds** of being
acknowledged. (You do not have to commit before acking — think about why.)

## Protocol

Terminology used below: a **byte** is 8 bits; a **nibble** is 4 bits (half a
byte); **uint16 BE** means an unsigned 16-bit integer stored big-endian
(most significant byte first). All multi-byte numbers in this protocol are
big-endian.

Every message, in both directions, is a **frame** with the same seven-part
structure:

### Frame layout

```
+-----------+--------+--------+--------------+---------+---------+-----------+
|   START   | LENGTH |  TYPE  |   PAYLOAD    | SERIAL  |   CRC   |   STOP    |
|  2 bytes  | 1 byte | 1 byte | LENGTH-5     | 2 bytes | 2 bytes |  2 bytes  |
| 0x78 0x78 |        |        | bytes        | uint16  | uint16  | 0x0D 0x0A |
+-----------+--------+--------+--------------+---------+---------+-----------+
```

| Field | Size | Meaning |
|-------|------|---------|
| START | 2 bytes | frame start marker, always `0x78 0x78` |
| LENGTH | 1 byte | the number of bytes from TYPE through CRC inclusive. Always payload size + 5 (TYPE 1 + payload + SERIAL 2 + CRC 2) |
| TYPE | 1 byte | what kind of packet this is: `0x01` = login, `0x12` = position. (Real GT06 documents call this the "protocol number".) |
| PAYLOAD | LENGTH-5 bytes | the packet's data; its layout depends on TYPE (see below) |
| SERIAL | 2 bytes, uint16 BE | packet counter. The device adds 1 for every packet it sends (logins included). The server echoes it back in the response, which is how the device knows *which* packet was received |
| CRC | 2 bytes, uint16 BE | checksum over the bytes from LENGTH through SERIAL inclusive. Algorithm: CRC-ITU (X.25 CRC-16, polynomial 0x1021 reflected, init `0xFFFF`, final XOR `0xFFFF`) — copy the reference code below, do not implement from the polynomial |
| STOP | 2 bytes | frame end marker, always `0x0D 0x0A` |

A real login frame split into those seven fields:

```
78 78 | 0D     | 01   | 08 62 49 60 51 23 45 67 | 00 01  | E8 48 | 0D 0A
START | LENGTH | TYPE | PAYLOAD (8 bytes)       | SERIAL | CRC   | STOP
        =13      login                            =1
```

CRC reference implementation:

```python
def crc16_x25(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0x8408 if crc & 1 else crc >> 1
    return (crc ^ 0xFFFF) & 0xFFFF
```

### Login packet — TYPE = 0x01

Payload: **8 bytes** — the 15-digit IMEI in BCD (binary-coded decimal: one
decimal digit per nibble, so two digits per byte). 15 digits need 16
nibbles, so prepend one `0` digit: encode the string `"0" + imei`.

```
imei=862496051234567  serial=1
frame:    78 78 0D 01 08 62 49 60 51 23 45 67 00 01 E8 48 0D 0A
response: 78 78 05 01 00 01 D9 DC 0D 0A
```

Byte-by-byte:

| bytes | field | value |
|-------|-------|-------|
| `78 78` | START | — |
| `0D` | LENGTH | 13 = TYPE(1) + payload(8) + SERIAL(2) + CRC(2) |
| `01` | TYPE | login |
| `08 62 49 60 51 23 45 67` | PAYLOAD | BCD digits `0862496051234567`, drop the leading pad digit → IMEI `862496051234567` |
| `00 01` | SERIAL | 1 |
| `E8 48` | CRC | crc16_x25 over `0D 01 08 62 49 60 51 23 45 67 00 01` |
| `0D 0A` | STOP | — |

### Position packet — TYPE = 0x12

Payload: **26 bytes** (offsets are within the payload, byte 0 = first
payload byte)

| offset | size | field | encoding |
|-------:|-----:|-------|----------|
| 0 | 6 bytes | date-time (UTC) | six unsigned bytes: year-2000, month, day, hour, minute, second |
| 6 | 1 byte | GPS info | high nibble: GPS data length, always `0xC`; low nibble: satellite count 0-15 |
| 7 | 4 bytes | latitude | uint32 BE = `abs(degrees) * 1,800,000`, rounded. Sign comes from the course/status field |
| 11 | 4 bytes | longitude | uint32 BE = `abs(degrees) * 1,800,000`, rounded. Sign comes from the course/status field |
| 15 | 1 byte | speed | km/h, 0-255 |
| 16 | 2 bytes | course/status | uint16 BE bit field — see bit table below |
| 18 | 2 bytes | MCC (ignore) | mobile country code |
| 20 | 1 byte | MNC (ignore) | mobile network code |
| 21 | 2 bytes | LAC (ignore) | location area code |
| 23 | 3 bytes | Cell ID (ignore) | cell tower id |

Course/status bit field (bit 0 = least significant bit, bit 15 = most
significant; bits 13-15 are always 0):

| bit | meaning |
|----:|---------|
| 12 | GPS fix valid (1 = valid) |
| 11 | longitude hemisphere: 1 = West (negative), 0 = East (positive) |
| 10 | latitude hemisphere: 1 = North (positive), 0 = South (negative) |
| 9-0 | course in degrees, 0-359 |

So: `latitude_degrees = raw / 1,800,000`, negate if bit 10 is 0;
`longitude_degrees = raw / 1,800,000`, negate if bit 11 is 1.

Worked example 1 (Delhi, north-east, valid fix):

```
frame:    78 78 1F 12 1A 07 0E 0A 1E 00 C9 03 11 E7 C2 08 48 9B F1 3E 14 91 01 94 2D 1A 2B 00 C3 D4 00 02 9C 9A 0D 0A
response: 78 78 05 12 00 02 81 B6 0D 0A
stored row: fix_time=2026-07-14 10:30:00 valid=1 lat=28.6139389 lon=77.2090228 speed=62 course=145 sats=9
```

Byte-by-byte:

| bytes | field | value |
|-------|-------|-------|
| `78 78` | START | — |
| `1F` | LENGTH | 31 = TYPE(1) + payload(26) + SERIAL(2) + CRC(2) |
| `12` | TYPE | position |
| `1A 07 0E 0A 1E 00` | date-time | 0x1A=26 → 2026, 07, 0x0E=14, 0x0A=10, 0x1E=30, 00 → 2026-07-14 10:30:00 UTC |
| `C9` | GPS info | high nibble 0xC (fixed), low nibble 9 → 9 satellites |
| `03 11 E7 C2` | latitude | 51,505,090 / 1,800,000 = 28.6139389; bit 10 set → North → +28.6139389 |
| `08 48 9B F1` | longitude | 138,976,241 / 1,800,000 = 77.2090228; bit 11 clear → East → +77.2090228 |
| `3E` | speed | 62 km/h |
| `14 91` | course/status | 0x1491 = `0001 0100 1001 0001`: bit 12 = 1 (valid), bit 11 = 0 (East), bit 10 = 1 (North), bits 9-0 = 0x091 = 145° |
| `01 94` `2D` `1A 2B` `00 C3 D4` | MCC MNC LAC CellID | consume and ignore (404, 45, …) |
| `00 02` | SERIAL | 2 |
| `9C 9A` | CRC | over LENGTH..SERIAL |
| `0D 0A` | STOP | — |

Worked example 2 (Buenos Aires — south-west, INVALID fix, edge values;
invalid fixes are still stored, with valid=0):

```
frame:    78 78 1F 12 1A 07 0E 0A 1E 00 CF 03 B6 6B 6C 06 43 7F 92 00 09 67 02 D2 07 01 02 03 04 05 00 03 8D DF 0D 0A
response: 78 78 05 12 00 03 90 3F 0D 0A
stored row: fix_time=2026-07-14 10:30:00 valid=0 lat=-34.6037222 lon=-58.3815922 speed=0 course=359 sats=15
```

Key bytes: course/status `09 67` = `0000 1001 0110 0111`: bit 12 = 0
(invalid fix), bit 11 = 1 (West → longitude negative), bit 10 = 0 (South →
latitude negative), bits 9-0 = 0x167 = 359°. GPS info `CF` → 15 satellites.
Speed `00` = 0 km/h.

### Server responses

Reply to **every valid login and position** with a 10-byte frame echoing the
packet's `TYPE` and `SERIAL`, with an empty payload (so `LENGTH = 0x05`):

```
78 78 | 05     | <TYPE> | <SERIAL: 2 bytes> | <CRC: 2 bytes> | 0D 0A
START | LENGTH | TYPE   | SERIAL (echoed)   | CRC            | STOP
```

### Rules (the tester checks every one of these)

1. **Login first.** The first packet on a connection is a login. Position
   packets received before a login on that connection: no response, no
   storage, keep the connection alive and keep parsing. Note: a device may
   send positions immediately after its login bytes **without waiting for
   your login response** (they can even arrive in the same TCP segment) —
   process the stream strictly in order, or positions will race past your
   login handling.
2. **Reconnect = re-login.** After a TCP drop a device logs in again on its
   new connection. Same IMEI must map to the same device row (no duplicate
   devices).
3. **TCP is a byte stream.** Multiple frames may arrive in one read; one
   frame may arrive a byte at a time. Handle both.
4. **Bad CRC = drop silently.** No response, no storage, continue with the
   next frame. (You may assume the START and LENGTH bytes are never
   corrupted, so framing always survives — corruption only ever hits the
   payload.)
5. **Duplicates happen.** Trackers retransmit packets whose ack got lost.
   The same (device, serial) may arrive again: **respond again**, but store
   the position **only once** (see the unique key in the schema).
6. **Unknown TYPE with a valid CRC:** consume the frame, no response, no
   storage. (Only `0x01` and `0x12` are meaningful in this challenge.)

## Storage

Load `schema.sql` (also created automatically by the provided
`docker-compose.yml`). Two tables:

- `devices(id, imei, created_at)` — one row per unique IMEI. **Your server
  creates this row itself** (auto-registration) the first time an IMEI logs
  in; there is no pre-loaded device list. When a known IMEI logs in again —
  reconnects happen constantly — it must map to its existing row, never a
  second one. (`INSERT IGNORE` + the `UNIQUE KEY (imei)` makes this a
  one-liner; cache the id in memory instead of querying it per packet.)
- `positions(id, device_id, serial, fix_time, valid, latitude, longitude,
  speed, course, satellites, server_time)` — one row per accepted position;
  `UNIQUE KEY (device_id, serial)` is your deduplication guarantee.

About serial numbers — read this before designing your dedup:

- A serial is only meaningful **per device**. Two different devices will
  routinely send the same serial numbers at the same time — that is why the
  unique key is `(device_id, serial)`, never serial alone.
- Serials start at an **arbitrary value** (not 1) and increment by 1 per
  packet **including logins**, so the position serials you store will have
  gaps. Do not assume they start anywhere or are consecutive.
- The same `(device, serial)` repeats only when a tracker retransmits a
  packet whose response it never received (rule 5).

Values must match the packet exactly: `fix_time` is the packet's UTC
date-time, latitude/longitude are the signed degree values shown above
(tester tolerance: 5e-7), speed/course/satellites/valid exact.

## Scoring (90 points + leaderboard)

The official tester (`tester/tester.py`) is public — run it yourself all
day. Official scoring runs the exact same tool, with a fresh random seed.

| Mode | Points | What it does |
|------|-------:|--------------|
| `basic` | 25 | schema check, login handshake, positions with edge-case values (S/W hemispheres, speed 0/255, course 0/359, sats 0/15, invalid fix), reconnect re-login, exact rows in MySQL |
| `stream` | 20 | coalesced frames, 1-7 byte fragmentation, corrupted CRC mid-stream, duplicate serial, position-before-login, abrupt-drop reconnect |
| `load` | 45 | **10,000 devices, 1 pos/s each, 30 s, 1%/s of devices drop + reconnect + retransmit.** All logins answered, zero position loss, p99 ack latency <= 1 s (after 5 s warmup), every row in MySQL field-exact (30 s visibility SLA; the tester is lenient and polls up to 60 s) — **and your server may hold at most 25 MySQL connections while under load** (sampled live via `Threads_connected`; production databases cap your pool, throwing connections at the problem is not an option) |
| `ramp` | leaderboard | device ladder 1k → 20k: the highest step you survive is your leaderboard number. Ties break on p99 latency |

```
python tester/tester.py basic   --host <you> --port 5023 --mysql-host <you>
python tester/tester.py stream  --host <you>
python tester/tester.py load    --host <you> --devices 10000 --interval 1 --duration 30
python tester/tester.py ramp    --host <you> --ladder 1000,2000,5000,10000,15000,20000
python tester/tester.py all     --host <you>       # basic + stream + load, /90
```

The tester needs Python 3.10+ and `pip install pymysql`. It reads your MySQL
to verify rows, so give it the same `--mysql-*` flags your server uses.

## Hints (read these when the load test hurts)

- 10,000 sockets: what does one-thread-per-connection cost? What are the
  alternatives in your language (event loop, NIO/Netty, goroutines, asyncio,
  virtual threads)?
- 10,000 positions/second: how many DB round trips is your server making per
  packet? What happens to a connection pool of 10 when each packet borrows a
  connection for a synchronous INSERT? How many round trips per second do
  you make if you batch 2,000 rows into one INSERT? (Remember: 25
  connections maximum. The reference solution passes the full load holding
  exactly one.)
- Look up `rewriteBatchedStatements` (JDBC), `executemany`, multi-row
  `INSERT`, `INSERT IGNORE`.
- How many times do you need to ask MySQL for the device id of the same
  IMEI?
- Does acking a packet require the row to be committed first? What does the
  30-second visibility SLA let you do?

## Provided files

```
PROBLEM.md            this file
QUICKSTART.md         setup steps + the recommended test workflow
schema.sql            the exact MySQL schema (scoring verifies against it)
docker-compose.yml    one-command MySQL with the schema pre-loaded
tester/tester.py      the official tester - the same one used for scoring
samples/frames.hex    19 example frames (3 devices; includes 1 corrupted
                      frame and 1 duplicate serial)
samples/responses.hex line-by-line expected response for each frame
                      ("-" = no response)
samples/decoded.csv   ground truth: every field of every sample frame
```

Follow QUICKSTART.md to get set up and testing. Good luck.
