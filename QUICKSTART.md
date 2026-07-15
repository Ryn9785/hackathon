# Quickstart — from zip to score

## Requirements (install these first)

| What | Check it works |
|------|----------------|
| Python 3.10+ | `python --version` |
| pymysql | `python -m pip install pymysql` |
| Docker Desktop (or a native MySQL 8) | `docker --version` |
| Your language toolchain (any language) | — |
| Laptop: 4+ cores / 8 GB RAM recommended | — |

## Setup (once, ~5 minutes)

```powershell
# 1. unzip, open a terminal inside the folder
cd hackathon

# 2. start MySQL (creates db `hackathon`, user hackathon/hackathon123,
#    loads schema.sql automatically)
docker compose up -d
docker compose ps          # STATUS must say running/healthy

# 3. install the tester dependency and sanity-check the tester itself
python -m pip install pymysql
python tester/tester.py selftest      # must print: all protocol vectors OK
```

**"ports are not available ... 3306" error?** Something on your machine
(XAMPP, an installed MySQL service) already uses port 3306. Two fixes:

- Easiest: use a different host port. Edit `docker-compose.yml`, change
  `"3306:3306"` to `"3310:3306"`, then `docker compose up -d` again — and
  from then on add `--mysql-port 3310` to every tester command and set
  `MYSQL_PORT=3310` for your server.
- Or stop the conflicting service while you work
  (Windows, admin terminal: `net stop MySQL84` — name from `Get-Service *mysql*`).

Verify either way with `docker compose ps` (STATUS running) before moving on.

No Docker? Use any MySQL 8 instead:

```sql
CREATE DATABASE hackathon;
CREATE USER 'hackathon'@'%' IDENTIFIED BY 'hackathon123';
GRANT ALL PRIVILEGES ON hackathon.* TO 'hackathon'@'%';
```
then load the schema: `mysql -u hackathon -phackathon123 hackathon < schema.sql`

## Build your server (see PROBLEM.md for the full spec)

Your server must:
- listen on TCP `0.0.0.0:5023` (env var `PORT` overrides)
- read MySQL settings from env vars `MYSQL_HOST`, `MYSQL_PORT`,
  `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB` — with the compose MySQL the
  defaults (127.0.0.1:3306, hackathon/hackathon123, db hackathon) just work,
  so you need to set nothing
- be started by ONE documented command (you will be asked to run it at
  scoring time)

Tip: before writing any networking, decode `samples/frames.hex` by hand and
compare with `samples/decoded.csv`. When your parser reproduces that CSV,
the hard part of parsing is done. `samples/responses.hex` shows the exact
bytes you must reply for each frame ("-" = reply nothing).

## Test loop (after every change)

```powershell
# start YOUR server in one terminal, then in another:

python tester/tester.py basic          # 25 pts - protocol + storage correctness
python tester/tester.py stream         # 20 pts - fragmentation, corruption, dedup
python tester/tester.py load --devices 1000 --duration 15     # warm-up scale
python tester/tester.py load --devices 10000 --interval 1 --duration 30   # 45 pts, the real target
python tester/tester.py ramp --ladder 1000,2000,5000,10000,15000,20000    # leaderboard

# full official score in one shot (/90):
python tester/tester.py all --devices 10000 --interval 1 --duration 30
```

- Every FAIL line prints why (e.g. `unacked=1200`, `p99=3500 ms`,
  `fix_time 2026-07-14 16:00:00 != 2026-07-14 10:30:00`). Fix, restart your
  server, rerun.
- Linux/macOS: the default open-file limit (`ulimit -n`, often 1024) is far
  below 10,000 sockets. The tester raises its own limit automatically, but
  **your server needs the same headroom** — run it with `ulimit -n 65535`
  in that shell (or the equivalent for your runtime) before the big load
  and ramp runs.
- Each run generates fresh random devices — you never need to clean the
  database between runs.
- Server and MySQL on other machines? Add
  `--host <server-ip> --mysql-host <db-ip>` to any command.

## Official scoring

When it is time to be scored, have ready: your MySQL up, your server
running, and the start command to show. Scoring runs the SAME tester (a
separate clean copy, fresh random seed):

```powershell
python tester/tester.py all  --devices 10000 --interval 1 --duration 30   # score /90
python tester/tester.py ramp                                              # leaderboard
```

Scoring: basic 25 + stream 20 + load 45 = 90 points. Leaderboard: highest
ramp step survived, ties broken by lower p99 latency.

Note: under load your server may hold at most **25 MySQL connections** — the
tester watches `Threads_connected` live while the load runs. Close your GUI
database clients during test runs so they don't count against you.
