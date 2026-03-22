#!/usr/bin/env python3
"""SensorHub v2 – Original DHT22 polling + new Air Quality & PIR push endpoints.
Preserves the existing schema (devices, readings, device_status) exactly as-is.
Adds new tables: air_devices, air_readings, pir_devices, pir_events, pir_sessions.
"""
import asyncio, json, logging, sqlite3, time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import io

DB_PATH = Path("/opt/sensorhub/data/sensorhub.db")
POLL_INTERVAL = 2
POLL_TIMEOUT = 1.5
CONFIG_PATH = Path("/opt/sensorhub/data/devices.json")

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("sensorhub")

# ══════════════════════════════════════════════════════════════════════════════
#  DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def get_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        -- ── Original DHT22 tables (unchanged) ────────────────────────────────
        CREATE TABLE IF NOT EXISTS devices (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, host TEXT NOT NULL UNIQUE,
            added_at TEXT NOT NULL DEFAULT (datetime('now')), enabled INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL,
            temperature REAL NOT NULL, humidity REAL NOT NULL, uptime INTEGER,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (device_id) REFERENCES devices(id)
        );
        CREATE INDEX IF NOT EXISTS idx_readings_device_time ON readings(device_id, recorded_at);
        CREATE INDEX IF NOT EXISTS idx_readings_time ON readings(recorded_at);
        CREATE TABLE IF NOT EXISTS device_status (
            device_id TEXT PRIMARY KEY, connected INTEGER NOT NULL DEFAULT 0,
            last_seen TEXT, last_error TEXT,
            FOREIGN KEY (device_id) REFERENCES devices(id)
        );

        -- ── Air Quality tables (new) ─────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS air_devices (
            device_id TEXT PRIMARY KEY,
            device_name TEXT DEFAULT '',
            first_seen TEXT NOT NULL DEFAULT (datetime('now')),
            last_seen TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS air_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            temperature REAL,
            humidity REAL,
            eco2 INTEGER,
            tvoc INTEGER,
            warmup_status TEXT DEFAULT 'unknown',
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_air_device ON air_readings(device_id);
        CREATE INDEX IF NOT EXISTS idx_air_ts ON air_readings(recorded_at);

        -- ── PIR Occupancy tables (new) ───────────────────────────────────────
        CREATE TABLE IF NOT EXISTS pir_devices (
            device_id TEXT PRIMARY KEY,
            device_name TEXT DEFAULT '',
            first_seen TEXT NOT NULL DEFAULT (datetime('now')),
            last_seen TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS pir_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            motion_detected INTEGER NOT NULL DEFAULT 0,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_pir_device ON pir_events(device_id);
        CREATE INDEX IF NOT EXISTS idx_pir_ts ON pir_events(recorded_at);

        CREATE TABLE IF NOT EXISTS pir_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            duration_seconds INTEGER DEFAULT 0,
            source TEXT DEFAULT 'auto'
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_device ON pir_sessions(device_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_start ON pir_sessions(start_time);

        -- ── Weather readings (stored from OpenWeatherMap) ────────────────────
        CREATE TABLE IF NOT EXISTS weather_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            temperature REAL,
            humidity REAL,
            feels_like REAL,
            pressure REAL,
            wind_speed REAL,
            description TEXT,
            icon TEXT,
            city TEXT,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_weather_ts ON weather_readings(recorded_at);
    """)
    conn.commit()
    conn.close()
    log.info(f"Database ready at {DB_PATH}")

def load_devices_from_file():
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM devices").fetchone()[0]
    if count > 0:
        conn.close()
        return
    if CONFIG_PATH.exists():
        try:
            devices = json.loads(CONFIG_PATH.read_text())
            for d in devices:
                conn.execute("INSERT OR IGNORE INTO devices (id, name, host) VALUES (?, ?, ?)",
                             (d["id"], d["name"], d["host"]))
                conn.execute("INSERT OR IGNORE INTO device_status (device_id) VALUES (?)", (d["id"],))
            conn.commit()
            log.info(f"Loaded {len(devices)} devices from {CONFIG_PATH}")
        except Exception as e:
            log.error(f"Failed to load devices config: {e}")
    conn.close()

# ══════════════════════════════════════════════════════════════════════════════
#  DHT22 POLLING (original, unchanged)
# ══════════════════════════════════════════════════════════════════════════════
async def poll_sensors():
    log.info(f"Sensor polling started (every {POLL_INTERVAL}s)")
    while True:
        conn = get_db()
        devices = conn.execute("SELECT id, name, host FROM devices WHERE enabled = 1").fetchall()
        conn.close()
        if devices:
            await asyncio.gather(
                *(poll_one_device(d["id"], d["name"], d["host"]) for d in devices),
                return_exceptions=True
            )
        await asyncio.sleep(POLL_INTERVAL)

async def poll_one_device(device_id, name, host):
    url = f"http://{host}/api/sensors/all"
    try:
        async with httpx.AsyncClient(timeout=POLL_TIMEOUT) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
        temp = data.get("temperature")
        hum = data.get("humidity")
        uptime = data.get("uptime")
        if temp is None or hum is None:
            raise ValueError("Missing temperature or humidity")
        if isinstance(temp, float) and temp != temp:
            raise ValueError("Sensor returned NaN")
        if isinstance(hum, float) and hum != hum:
            raise ValueError("Sensor returned NaN")
        conn = get_db()
        conn.execute(
            "INSERT INTO readings (device_id, temperature, humidity, uptime) VALUES (?, ?, ?, ?)",
            (device_id, float(temp), float(hum), uptime)
        )
        conn.execute(
            """INSERT INTO device_status (device_id, connected, last_seen, last_error)
               VALUES (?, 1, datetime('now'), NULL)
               ON CONFLICT(device_id) DO UPDATE SET connected = 1, last_seen = datetime('now'), last_error = NULL""",
            (device_id,)
        )
        conn.commit()
        conn.close()
        log.debug(f"[{name}] {temp:.1f}C {hum:.1f}% uptime={uptime}s")
    except Exception as e:
        conn = get_db()
        conn.execute(
            """INSERT INTO device_status (device_id, connected, last_error)
               VALUES (?, 0, ?)
               ON CONFLICT(device_id) DO UPDATE SET connected = 0, last_error = ?""",
            (device_id, str(e), str(e))
        )
        conn.commit()
        conn.close()
        log.warning(f"[{name}] Poll failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  APP LIFESPAN
# ══════════════════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app):
    init_db()
    load_devices_from_file()
    task = asyncio.create_task(poll_sensors())
    weather_task = asyncio.create_task(poll_weather())
    yield
    task.cancel()
    weather_task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    try:
        await weather_task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="SensorHub API", version="2.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ══════════════════════════════════════════════════════════════════════════════
#  ORIGINAL DHT22 ENDPOINTS (unchanged)
# ══════════════════════════════════════════════════════════════════════════════
class DeviceCreate(BaseModel):
    name: str
    host: str

class DeviceUpdate(BaseModel):
    name: Optional[str] = None
    host: Optional[str] = None
    enabled: Optional[bool] = None

@app.get("/api/devices")
def list_devices():
    conn = get_db()
    rows = conn.execute(
        """SELECT d.id, d.name, d.host, d.enabled, d.added_at,
                  s.last_seen, s.last_error,
                  CASE WHEN s.last_seen IS NOT NULL
                       AND (julianday('now') - julianday(s.last_seen)) * 86400 < 30
                  THEN 1 ELSE 0 END AS connected
           FROM devices d LEFT JOIN device_status s ON d.id = s.device_id
           ORDER BY d.added_at"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/api/devices")
def add_device(device: DeviceCreate):
    device_id = f"dev_{int(time.time() * 1000)}"
    conn = get_db()
    try:
        conn.execute("INSERT INTO devices (id, name, host) VALUES (?, ?, ?)",
                     (device_id, device.name, device.host))
        conn.execute("INSERT INTO device_status (device_id) VALUES (?)", (device_id,))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(400, "A device with this hostname already exists")
    conn.close()
    return {"id": device_id, "name": device.name, "host": device.host}

@app.put("/api/devices/{device_id}")
def update_device(device_id: str, update: DeviceUpdate):
    conn = get_db()
    device = conn.execute("SELECT * FROM devices WHERE id = ?", (device_id,)).fetchone()
    if not device:
        conn.close()
        raise HTTPException(404, "Device not found")
    if update.name is not None:
        conn.execute("UPDATE devices SET name = ? WHERE id = ?", (update.name, device_id))
    if update.host is not None:
        conn.execute("UPDATE devices SET host = ? WHERE id = ?", (update.host, device_id))
    if update.enabled is not None:
        conn.execute("UPDATE devices SET enabled = ? WHERE id = ?", (int(update.enabled), device_id))
    conn.commit()
    conn.close()
    return {"status": "updated"}

@app.delete("/api/devices/{device_id}")
def delete_device(device_id: str):
    conn = get_db()
    conn.execute("DELETE FROM readings WHERE device_id = ?", (device_id,))
    conn.execute("DELETE FROM device_status WHERE device_id = ?", (device_id,))
    conn.execute("DELETE FROM devices WHERE id = ?", (device_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}

@app.get("/api/readings/latest")
def latest_readings():
    conn = get_db()
    rows = conn.execute(
        """SELECT r.*, d.name as device_name, d.host as device_host
           FROM readings r JOIN devices d ON r.device_id = d.id
           WHERE r.id IN (SELECT MAX(id) FROM readings GROUP BY device_id)
           ORDER BY r.recorded_at DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/readings")
def get_readings(device_id: Optional[str] = None, hours: Optional[int] = Query(None),
                 days: Optional[int] = Query(None), limit: int = Query(500, le=10000), offset: int = 0):
    conn = get_db()
    conditions = []
    params = []
    if device_id:
        conditions.append("r.device_id = ?")
        params.append(device_id)
    if hours:
        conditions.append("r.recorded_at >= datetime('now', ?)")
        params.append(f"-{hours} hours")
    elif days:
        conditions.append("r.recorded_at >= datetime('now', ?)")
        params.append(f"-{days} days")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = conn.execute(
        f"SELECT r.*, d.name as device_name FROM readings r JOIN devices d ON r.device_id = d.id {where} ORDER BY r.recorded_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    count = conn.execute(f"SELECT COUNT(*) FROM readings r {where}", params).fetchone()[0]
    conn.close()
    return {"readings": [dict(r) for r in rows], "total": count}

@app.get("/api/readings/stats")
def reading_stats(device_id: Optional[str] = None):
    conn = get_db()
    condition = "WHERE device_id = ?" if device_id else ""
    params = [device_id] if device_id else []
    row = conn.execute(
        f"""SELECT COUNT(*) AS total_readings,
                   ROUND(AVG(temperature),2) AS avg_temp, ROUND(MIN(temperature),2) AS min_temp,
                   ROUND(MAX(temperature),2) AS max_temp, ROUND(AVG(humidity),2) AS avg_hum,
                   ROUND(MIN(humidity),2) AS min_hum, ROUND(MAX(humidity),2) AS max_hum,
                   MIN(recorded_at) AS first_reading, MAX(recorded_at) AS last_reading
            FROM readings {condition}""",
        params
    ).fetchone()
    conn.close()
    return dict(row)

@app.delete("/api/readings")
def delete_readings(device_id: Optional[str] = None):
    conn = get_db()
    if device_id:
        conn.execute("DELETE FROM readings WHERE device_id = ?", (device_id,))
    else:
        conn.execute("DELETE FROM readings")
    conn.commit()
    count = conn.execute("SELECT changes()").fetchone()[0]
    conn.close()
    return {"deleted": count}

@app.get("/api/readings/export/csv")
def export_csv(device_id: Optional[str] = None, days: Optional[int] = None):
    conn = get_db()
    conditions = []
    params = []
    if device_id:
        conditions.append("r.device_id = ?")
        params.append(device_id)
    if days:
        conditions.append("r.recorded_at >= datetime('now', ?)")
        params.append(f"-{days} days")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = conn.execute(
        f"SELECT r.recorded_at, d.name, r.temperature, r.humidity, r.uptime FROM readings r JOIN devices d ON r.device_id = d.id {where} ORDER BY r.recorded_at ASC",
        params
    ).fetchall()
    conn.close()
    output = io.StringIO()
    output.write("Timestamp,Device,Temperature (C),Humidity (%),Uptime (s)\n")
    for r in rows:
        output.write(f"{r['recorded_at']},{r['name']},{r['temperature']},{r['humidity']},{r['uptime'] or ''}\n")
    output.seek(0)
    filename = f"sensorhub_export_{datetime.now().strftime('%Y%m%d')}.csv"
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})

@app.post("/api/ingest")
def ingest_reading(data: dict):
    device_id = data.get("device_id", "unknown")
    device_name = data.get("device_name", "Unknown Sensor")
    temp = data.get("temperature")
    hum = data.get("humidity")
    if temp is None or hum is None:
        raise HTTPException(400, "Missing temperature or humidity")
    conn = get_db()
    existing = conn.execute("SELECT id FROM devices WHERE id = ?", (device_id,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO devices (id, name, host) VALUES (?, ?, ?)",
                     (device_id, device_name, device_id + ".local"))
        conn.execute("INSERT INTO device_status (device_id) VALUES (?)", (device_id,))
    conn.execute("INSERT INTO readings (device_id, temperature, humidity) VALUES (?, ?, ?)",
                 (device_id, float(temp), float(hum)))
    conn.execute(
        """INSERT INTO device_status (device_id, connected, last_seen, last_error)
           VALUES (?, 1, datetime('now'), NULL)
           ON CONFLICT(device_id) DO UPDATE SET connected = 1, last_seen = datetime('now'), last_error = NULL""",
        (device_id,)
    )
    conn.commit()
    conn.close()
    return {"status": "ok"}

# ══════════════════════════════════════════════════════════════════════════════
#  AIR QUALITY ENDPOINTS (new)
# ══════════════════════════════════════════════════════════════════════════════
class AirReading(BaseModel):
    device_id: str
    device_name: str = ""
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    eco2: Optional[int] = None
    tvoc: Optional[int] = None
    warmup_status: str = "unknown"

@app.post("/api/air/ingest")
def ingest_air(r: AirReading):
    conn = get_db()
    conn.execute(
        """INSERT INTO air_devices (device_id, device_name, last_seen)
           VALUES (?,?,datetime('now'))
           ON CONFLICT(device_id) DO UPDATE SET device_name=excluded.device_name, last_seen=datetime('now')""",
        (r.device_id, r.device_name)
    )
    conn.execute(
        "INSERT INTO air_readings (device_id, temperature, humidity, eco2, tvoc, warmup_status) VALUES (?,?,?,?,?,?)",
        (r.device_id, r.temperature, r.humidity, r.eco2, r.tvoc, r.warmup_status)
    )
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.get("/api/air/readings")
def get_air_readings(device_id: str = None, hours: int = 24, limit: int = 1000):
    conn = get_db()
    since = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    if device_id:
        rows = conn.execute(
            "SELECT * FROM air_readings WHERE device_id=? AND recorded_at>=? ORDER BY recorded_at DESC LIMIT ?",
            (device_id, since, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM air_readings WHERE recorded_at>=? ORDER BY recorded_at DESC LIMIT ?",
            (since, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/air/latest")
def get_air_latest(device_id: str = None):
    conn = get_db()
    if device_id:
        row = conn.execute(
            "SELECT * FROM air_readings WHERE device_id=? ORDER BY recorded_at DESC LIMIT 1",
            (device_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else {}
    else:
        rows = conn.execute(
            """SELECT ar.* FROM air_readings ar
               INNER JOIN (SELECT device_id, MAX(recorded_at) as max_ts FROM air_readings GROUP BY device_id) g
               ON ar.device_id = g.device_id AND ar.recorded_at = g.max_ts"""
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

@app.get("/api/air/devices")
def get_air_devices():
    conn = get_db()
    rows = conn.execute(
        """SELECT *,
                  CASE WHEN last_seen IS NOT NULL
                       AND (julianday('now') - julianday(last_seen)) * 86400 < 30
                  THEN 1 ELSE 0 END AS connected
           FROM air_devices ORDER BY last_seen DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.delete("/api/air/readings")
def delete_air_readings(device_id: str = None):
    conn = get_db()
    if device_id:
        conn.execute("DELETE FROM air_readings WHERE device_id=?", (device_id,))
    else:
        conn.execute("DELETE FROM air_readings")
    conn.commit()
    conn.close()
    return {"status": "ok"}

# ══════════════════════════════════════════════════════════════════════════════
#  PIR OCCUPANCY ENDPOINTS (new)
# ══════════════════════════════════════════════════════════════════════════════
class PIREvent(BaseModel):
    device_id: str
    device_name: str = ""
    motion_detected: bool = False

@app.post("/api/pir/ingest")
def ingest_pir(r: PIREvent):
    conn = get_db()
    conn.execute(
        """INSERT INTO pir_devices (device_id, device_name, last_seen)
           VALUES (?,?,datetime('now'))
           ON CONFLICT(device_id) DO UPDATE SET device_name=excluded.device_name, last_seen=datetime('now')""",
        (r.device_id, r.device_name)
    )
    conn.execute("INSERT INTO pir_events (device_id, motion_detected) VALUES (?,?)",
                 (r.device_id, int(r.motion_detected)))

    open_session = conn.execute(
        "SELECT id, start_time FROM pir_sessions WHERE device_id=? AND end_time IS NULL ORDER BY start_time DESC LIMIT 1",
        (r.device_id,)
    ).fetchone()

    if r.motion_detected:
        if not open_session:
            conn.execute(
                "INSERT INTO pir_sessions (device_id, start_time, source) VALUES (?,datetime('now'),'auto')",
                (r.device_id,)
            )
    else:
        if open_session:
            conn.execute(
                """UPDATE pir_sessions SET
                   end_time = datetime('now'),
                   duration_seconds = CAST((julianday('now') - julianday(start_time)) * 86400 AS INTEGER)
                   WHERE id = ?""",
                (open_session["id"],)
            )

    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.get("/api/pir/sessions")
def get_pir_sessions(device_id: str = None, hours: int = 168, limit: int = 500):
    conn = get_db()
    since = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    if device_id:
        rows = conn.execute(
            "SELECT * FROM pir_sessions WHERE device_id=? AND start_time>=? AND end_time IS NOT NULL ORDER BY start_time DESC LIMIT ?",
            (device_id, since, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM pir_sessions WHERE start_time>=? AND end_time IS NOT NULL ORDER BY start_time DESC LIMIT ?",
            (since, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/pir/sessions/active")
def get_pir_active_sessions(device_id: str = None):
    conn = get_db()
    if device_id:
        rows = conn.execute(
            "SELECT * FROM pir_sessions WHERE device_id=? AND end_time IS NULL", (device_id,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM pir_sessions WHERE end_time IS NULL").fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/pir/stats")
def get_pir_stats(device_id: str = None):
    conn = get_db()
    where_dev = " AND device_id=?" if device_id else ""
    params_dev = (device_id,) if device_id else ()

    total = conn.execute(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(duration_seconds),0) as secs FROM pir_sessions WHERE end_time IS NOT NULL" + where_dev,
        params_dev
    ).fetchone()

    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    today = conn.execute(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(duration_seconds),0) as secs FROM pir_sessions WHERE end_time IS NOT NULL AND start_time>=?" + where_dev,
        (today_start,) + params_dev
    ).fetchone()

    week_start = (datetime.utcnow() - timedelta(days=datetime.utcnow().weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).strftime("%Y-%m-%d %H:%M:%S")
    week = conn.execute(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(duration_seconds),0) as secs FROM pir_sessions WHERE end_time IS NOT NULL AND start_time>=?" + where_dev,
        (week_start,) + params_dev
    ).fetchone()

    month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    month = conn.execute(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(duration_seconds),0) as secs FROM pir_sessions WHERE end_time IS NOT NULL AND start_time>=?" + where_dev,
        (month_start,) + params_dev
    ).fetchone()

    conn.close()
    return {
        "total_sessions": total["cnt"], "total_seconds": total["secs"],
        "today_sessions": today["cnt"], "today_seconds": today["secs"],
        "week_sessions": week["cnt"], "week_seconds": week["secs"],
        "month_sessions": month["cnt"], "month_seconds": month["secs"],
    }

@app.get("/api/pir/events")
def get_pir_events(device_id: str = None, hours: int = 24, limit: int = 500):
    conn = get_db()
    since = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    if device_id:
        rows = conn.execute(
            "SELECT * FROM pir_events WHERE device_id=? AND recorded_at>=? ORDER BY recorded_at DESC LIMIT ?",
            (device_id, since, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM pir_events WHERE recorded_at>=? ORDER BY recorded_at DESC LIMIT ?",
            (since, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/pir/devices")
def get_pir_devices():
    conn = get_db()
    rows = conn.execute(
        """SELECT *,
                  CASE WHEN last_seen IS NOT NULL
                       AND (julianday('now') - julianday(last_seen)) * 86400 < 30
                  THEN 1 ELSE 0 END AS connected
           FROM pir_devices ORDER BY last_seen DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.delete("/api/pir/sessions")
def delete_pir_sessions(device_id: str = None):
    conn = get_db()
    if device_id:
        conn.execute("DELETE FROM pir_sessions WHERE device_id=?", (device_id,))
        conn.execute("DELETE FROM pir_events WHERE device_id=?", (device_id,))
    else:
        conn.execute("DELETE FROM pir_sessions")
        conn.execute("DELETE FROM pir_events")
    conn.commit()
    conn.close()
    return {"status": "ok"}

# ══════════════════════════════════════════════════════════════════════════════
#  WEATHER API (OpenWeatherMap proxy + storage)
# ══════════════════════════════════════════════════════════════════════════════
WEATHER_CONFIG_PATH = Path("/opt/sensorhub/data/weather.json")

def load_weather_config():
    if WEATHER_CONFIG_PATH.exists():
        try:
            return json.loads(WEATHER_CONFIG_PATH.read_text())
        except:
            pass
    return {"api_key": "", "city": "", "lat": "", "lon": "", "interval_minutes": 15}

def save_weather_config(cfg):
    WEATHER_CONFIG_PATH.write_text(json.dumps(cfg, indent=2))

async def fetch_and_store_weather():
    """Fetch weather from OpenWeatherMap and store in DB."""
    cfg = load_weather_config()
    if not cfg.get("api_key") or (not cfg.get("city") and not (cfg.get("lat") and cfg.get("lon"))):
        return None

    base = "https://api.openweathermap.org/data/2.5/weather"
    params = {"appid": cfg["api_key"], "units": "metric"}
    if cfg.get("lat") and cfg.get("lon"):
        params["lat"] = cfg["lat"]
        params["lon"] = cfg["lon"]
    else:
        params["q"] = cfg["city"]

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(base, params=params)
            resp.raise_for_status()
            data = resp.json()

        weather = {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "feels_like": data["main"]["feels_like"],
            "pressure": data["main"]["pressure"],
            "description": data["weather"][0]["description"],
            "icon": data["weather"][0]["icon"],
            "wind_speed": data["wind"]["speed"],
            "city": data.get("name", cfg.get("city", "")),
            "country": data.get("sys", {}).get("country", ""),
        }

        # Store in DB
        conn = get_db()
        conn.execute(
            """INSERT INTO weather_readings (temperature, humidity, feels_like, pressure, wind_speed, description, icon, city)
               VALUES (?,?,?,?,?,?,?,?)""",
            (weather["temperature"], weather["humidity"], weather["feels_like"],
             weather["pressure"], weather["wind_speed"], weather["description"],
             weather["icon"], weather["city"])
        )
        conn.commit()
        conn.close()
        log.info(f"[Weather] {weather['city']}: {weather['temperature']}°C, {weather['description']}")
        return weather
    except Exception as e:
        log.warning(f"[Weather] Fetch failed: {e}")
        return None

async def poll_weather():
    """Background task: fetch weather at configured interval."""
    log.info("Weather polling started")
    await asyncio.sleep(5)  # Wait for startup
    while True:
        cfg = load_weather_config()
        interval = max(cfg.get("interval_minutes", 15), 1) * 60
        await fetch_and_store_weather()
        await asyncio.sleep(interval)

@app.get("/api/weather/config")
def get_weather_config_endpoint():
    cfg = load_weather_config()
    masked = cfg.copy()
    if masked.get("api_key"):
        k = masked["api_key"]
        masked["api_key_masked"] = k[:4] + "****" + k[-4:] if len(k) > 8 else "****"
        masked["api_key_set"] = True
    else:
        masked["api_key_masked"] = ""
        masked["api_key_set"] = False
    masked.pop("api_key", None)
    return masked

@app.post("/api/weather/config")
def set_weather_config(data: dict):
    cfg = load_weather_config()
    if "api_key" in data and data["api_key"]:
        cfg["api_key"] = data["api_key"]
    if "city" in data:
        cfg["city"] = data["city"]
    if "lat" in data:
        cfg["lat"] = data["lat"]
    if "lon" in data:
        cfg["lon"] = data["lon"]
    if "interval_minutes" in data:
        cfg["interval_minutes"] = int(data["interval_minutes"])
    save_weather_config(cfg)
    return {"status": "ok"}

@app.get("/api/weather/current")
async def get_current_weather():
    cfg = load_weather_config()
    if not cfg.get("api_key"):
        raise HTTPException(400, "No API key configured. Go to weather settings.")
    if not cfg.get("city") and not (cfg.get("lat") and cfg.get("lon")):
        raise HTTPException(400, "No location configured. Go to weather settings.")

    base = "https://api.openweathermap.org/data/2.5/weather"
    params = {"appid": cfg["api_key"], "units": "metric"}
    if cfg.get("lat") and cfg.get("lon"):
        params["lat"] = cfg["lat"]
        params["lon"] = cfg["lon"]
    else:
        params["q"] = cfg["city"]

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(base, params=params)
            resp.raise_for_status()
            data = resp.json()

        return {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "feels_like": data["main"]["feels_like"],
            "pressure": data["main"]["pressure"],
            "description": data["weather"][0]["description"],
            "icon": data["weather"][0]["icon"],
            "wind_speed": data["wind"]["speed"],
            "city": data.get("name", cfg.get("city", "")),
            "country": data.get("sys", {}).get("country", ""),
            "fetched_at": datetime.utcnow().isoformat(),
            "interval_minutes": cfg.get("interval_minutes", 15),
        }
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, f"OpenWeatherMap error: {e.response.text}")
    except Exception as e:
        raise HTTPException(502, f"Failed to fetch weather: {str(e)}")

@app.get("/api/weather/readings")
def get_weather_readings(hours: int = 24, limit: int = 2000):
    conn = get_db()
    since = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        "SELECT * FROM weather_readings WHERE recorded_at>=? ORDER BY recorded_at DESC LIMIT ?",
        (since, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/weather/stats")
def get_weather_stats():
    conn = get_db()
    row = conn.execute(
        """SELECT COUNT(*) as total,
                  ROUND(AVG(temperature),1) as avg_temp, ROUND(MIN(temperature),1) as min_temp, ROUND(MAX(temperature),1) as max_temp,
                  ROUND(AVG(humidity),1) as avg_hum, ROUND(MIN(humidity),1) as min_hum, ROUND(MAX(humidity),1) as max_hum,
                  MIN(recorded_at) as first_reading, MAX(recorded_at) as last_reading
           FROM weather_readings"""
    ).fetchone()
    conn.close()
    return dict(row)

@app.delete("/api/weather/readings")
def delete_weather_readings():
    conn = get_db()
    conn.execute("DELETE FROM weather_readings")
    conn.commit()
    conn.close()
    return {"status": "ok"}

# ══════════════════════════════════════════════════════════════════════════════
#  HEALTH
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/health")
def health():
    conn = get_db()
    rc = conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    dc = conn.execute("SELECT COUNT(*) FROM devices").fetchone()[0]
    ac = conn.execute("SELECT COUNT(*) FROM air_readings").fetchone()[0]
    pc = conn.execute("SELECT COUNT(*) FROM pir_sessions").fetchone()[0]
    conn.close()
    return {
        "status": "ok", "version": "2.1",
        "devices": dc, "total_readings": rc,
        "air_readings": ac, "pir_sessions": pc,
        "poll_interval": POLL_INTERVAL
    }
