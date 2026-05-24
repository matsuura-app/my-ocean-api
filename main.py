import os
import json
import sqlite3
import threading

from datetime import datetime, timedelta

import numpy as np
import requests

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# CONFIG
# =========================================================

MSIL_API_KEY = os.getenv("MSIL_API_KEY")

DATABASE = "cache.db"

UMISHIRU_URL = (
    "https://portal.msil.go.jp/"
    "msil/OgpProvider/czm/current_forecast_grid.aspx"
)

KEEP_ALIVE_URL = os.getenv(
    "KEEP_ALIVE_URL",
    "https://my-ocean-api.onrender.com"
)

session = requests.Session()

# =========================================================
# FASTAPI
# =========================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# SQLITE
# =========================================================

db_lock = threading.Lock()

def init_db():

    with sqlite3.connect(DATABASE) as conn:

        conn.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
        """)

        conn.commit()

init_db()

def set_cache(key, value):

    with db_lock:

        with sqlite3.connect(DATABASE) as conn:

            conn.execute(
                """
                INSERT OR REPLACE INTO cache
                (key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                (
                    key,
                    json.dumps(value),
                    datetime.utcnow().isoformat()
                )
            )

            conn.commit()

def get_cache(key, max_age_minutes=30):

    with db_lock:

        with sqlite3.connect(DATABASE) as conn:

            cur = conn.execute(
                """
                SELECT value, updated_at
                FROM cache
                WHERE key=?
                """,
                (key,)
            )

            row = cur.fetchone()

            if not row:
                return None

            value, updated_at = row

            updated_at = datetime.fromisoformat(updated_at)

            if (
                datetime.utcnow() - updated_at
                > timedelta(minutes=max_age_minutes)
            ):
                return None

            return json.loads(value)

# =========================================================
# UTIL
# =========================================================

def safe_float(v, default=0.0):

    try:

        v = float(v)

        if np.isnan(v):
            return default

        if np.isinf(v):
            return default

        return v

    except Exception:

        return default

# =========================================================
# KEEP ALIVE
# =========================================================

def keep_alive_loop():

    while True:

        try:

            requests.get(
                KEEP_ALIVE_URL,
                timeout=10
            )

            print(
                "💓 KEEP ALIVE",
                flush=True
            )

        except Exception as e:

            print(
                f"⚠️ KEEP ALIVE ERROR: {e}",
                flush=True
            )

        threading.Event().wait(600)

# =========================================================
# UMISHIRU
# =========================================================

def fetch_umishiru_data():

    cache_key = "umishiru"

    cached = get_cache(
        cache_key,
        max_age_minutes=30
    )

    if cached:

        print(
            "💾 UMISHIRU CACHE HIT",
            flush=True
        )

        return cached

    print(
        "🌐 FETCH UMISHIRU",
        flush=True
    )

    headers = {}

    if MSIL_API_KEY:

        headers["Ocp-Apim-Subscription-Key"] = (
            MSIL_API_KEY
        )

    try:

        r = session.get(
            UMISHIRU_URL,
            headers=headers,
            timeout=(5, 15)
        )

        if r.status_code == 200:

            data = r.json()

            set_cache(
                cache_key,
                data
            )

            return data

        print(
            f"⚠️ UMISHIRU STATUS {r.status_code}",
            flush=True
        )

    except Exception as e:

        print(
            f"⚠️ UMISHIRU ERROR: {e}",
            flush=True
        )

    return cached

# =========================================================
# FIND NEAREST
# =========================================================

def find_nearest_umishiru(
    lat,
    lon,
    data
):

    if not data:
        return None

    features = data.get(
        "features",
        []
    )

    nearest = None

    min_dist = float("inf")

    for feature in features:

        try:

            geom = feature.get(
                "geometry",
                {}
            )

            if geom.get("type") != "Point":
                continue

            coords = geom.get(
                "coordinates",
                []
            )

            if len(coords) < 2:
                continue

            f_lon = float(coords[0])
            f_lat = float(coords[1])

            dist = (
                (lat - f_lat) ** 2
                + (lon - f_lon) ** 2
            )

            if dist < min_dist:

                min_dist = dist
                nearest = feature

        except Exception:
            continue

    # 約15km
    if min_dist > 0.02:
        return None

    return nearest

# =========================================================
# WEATHER
# =========================================================

def fetch_weather(lat, lon):

    url = (
        "https://api.open-meteo.com/v1/forecast"
    )

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": (
            "temperature_2m,"
            "windspeed_10m,"
            "winddirection_10m,"
            "weathercode"
        ),
        "forecast_days": 3,
        "timezone": "Asia/Tokyo"
    }

    try:

        r = session.get(
            url,
            params=params,
            timeout=(5, 15)
        )

        if r.status_code == 200:
            return r.json()

    except Exception as e:

        print(
            f"⚠️ WEATHER ERROR: {e}",
            flush=True
        )

    return None

# =========================================================
# ROOT
# =========================================================

@app.get("/")
def root():

    return {
        "status": "ok",
        "server": "marine-final-v1"
    }

# =========================================================
# FORECAST
# =========================================================

@app.get("/forecast")
def forecast(
    lat: float = Query(...),
    lon: float = Query(...)
):

    print(
        f"📡 REQUEST lat={lat} lon={lon}",
        flush=True
    )

    weather = fetch_weather(
        lat,
        lon
    )

    umishiru = fetch_umishiru_data()

    current = None

    if umishiru:

        nearest = find_nearest_umishiru(
            lat,
            lon,
            umishiru
        )

        if nearest:

            props = nearest.get(
                "properties",
                {}
            )

            current = {
                "speed": safe_float(
                    props.get("speed", 0)
                ),
                "direction": safe_float(
                    props.get("direction", 0)
                )
            }

    return {
        "status": "success",
        "lat": lat,
        "lon": lon,
        "current": current,
        "weather": weather
    }

# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    threading.Thread(
        target=keep_alive_loop,
        daemon=True
    ).start()
