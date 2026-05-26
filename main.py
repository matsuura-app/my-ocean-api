import os
import threading
import sqlite3
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import requests
import xarray as xr
import numpy as np

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# CONFIG
# =========================================================

API_KEY = os.getenv("MSIL_API_KEY")

JST = timezone(timedelta(hours=9))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

session = requests.Session()
session.headers.update({
    "Origin": "https://my-ocean-api.onrender.com",
    "Referer": "https://my-ocean-api.onrender.com/",
    "User-Agent": "Mozilla/5.0"
})

# =========================================================
# LOCK & CACHE
# =========================================================

lock = threading.Lock()

umishiru_cache = {}

forecast_cache = {}
CACHE_TTL = 1800

# =========================================================
# HYCOM CONFIG
# =========================================================

DATA_URL = (
    "https://tds.hycom.org/thredds/dodsC/"
    "FMRC_ESPC-D-V02_uv3z/"
    "FMRC_ESPC-D-V02_uv3z_best.ncd"
)

ds_local = None
hycom_ready = False

# =========================================================
# SQLITE
# =========================================================

def get_conn():
    conn = sqlite3.connect(
        "tides.db",
        timeout=10,
        check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tides (
        point TEXT,
        datetime TEXT,
        height REAL,
        PRIMARY KEY(point, datetime)
    )
    """)

    conn.commit()
    conn.close()

# =========================================================
# HYCOM LOAD
# =========================================================

def load_hycom():

    global ds_local, hycom_ready

    print("HYCOM loading...", flush=True)

    try:

        ds_local = xr.open_dataset(
            DATA_URL,
            engine="netcdf4",
            decode_times=False
        ).sel(
            lat=slice(30, 46),
            lon=slice(129, 146)
        )

        hycom_ready = True

        print("HYCOM ready", flush=True)

    except Exception as e:

        print("HYCOM load error:", e, flush=True)

# =========================================================
# HYCOM CURRENT
# =========================================================

def get_from_hycom(lat, lon):

    if not hycom_ready or ds_local is None:
        return {
            "status": "loading",
            "message": "HYCOM initializing"
        }

    try:

        subset = ds_local.sel(
            lat=slice(lat - 0.2, lat + 0.2),
            lon=slice(lon - 0.2, lon + 0.2)
        ).isel(time=0)

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u_array = subset["water_u"].values
        v_array = subset["water_v"].values

        valid = ~np.isnan(u_array) & ~np.isnan(v_array)

        if not np.any(valid):
            return {
                "status": "error",
                "message": "land"
            }

        idx = np.argwhere(valid)[0]

        u = float(u_array[idx[0], idx[1]])
        v = float(v_array[idx[0], idx[1]])

        speed = np.sqrt(u**2 + v**2) * 1.94384

        direction = (
            np.degrees(np.arctan2(v, u)) + 360
        ) % 360

        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "direction": round(direction, 1),
            "source": "HYCOM"
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

# =========================================================
# WEATHER
# =========================================================

def fetch_weather_logic(lat, lon):

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": (
            "temperature_2m,"
            "windspeed_10m,"
            "winddirection_10m,"
            "weathercode"
        ),
        "forecast_days": 7,
        "timezone": "Asia/Tokyo"
    }

    try:

        r = session.get(
            "https://api.open-meteo.com/v1/forecast",
            params=params,
            timeout=10
        )

        if r.status_code == 200:
            return r.json()

    except:
        pass

    return None

# =========================================================
# API
# =========================================================

@app.get("/")
def root():

    return {
        "status": "ok",
        "server": "marine-final"
    }

@app.get("/current")
def current(
    lat: float = Query(...),
    lon: float = Query(...)
):

    return get_from_hycom(lat, lon)

@app.get("/forecast")
def forecast(
    lat: float = Query(...),
    lon: float = Query(...)
):

    if not hycom_ready or ds_local is None:
        return {
            "status": "loading",
            "data": []
        }

    key = f"{round(lat,2)}_{round(lon,2)}"

    now = datetime.utcnow().timestamp()

    with lock:

        cache = forecast_cache.get(key)

        if cache and now - cache["time"] < CACHE_TTL:
            return cache["data"]

    try:

        subset = ds_local.sel(
            lat=slice(lat - 0.2, lat + 0.2),
            lon=slice(lon - 0.2, lon + 0.2)
        )

        results = []

        max_time = subset.sizes["time"]

        for h in range(min(48, max_time)):

            data = subset.isel(time=h)

            if "depth" in data.dims:
                data = data.isel(depth=0)

            u_array = data["water_u"].values
            v_array = data["water_v"].values

            valid = ~np.isnan(u_array) & ~np.isnan(v_array)

            if not np.any(valid):

                results.append({
                    "time": h,
                    "speed": 0.0,
                    "direction": 0.0
                })

                continue

            idx = np.argwhere(valid)[0]

            u = float(u_array[idx[0], idx[1]])
            v = float(v_array[idx[0], idx[1]])

            speed = np.sqrt(u**2 + v**2) * 1.94384

            direction = (
                np.degrees(np.arctan2(v, u)) + 360
            ) % 360

            results.append({
                "time": h,
                "speed": round(speed, 2),
                "direction": round(direction, 1)
            })

        response = {
            "status": "success",
            "data": results
        }

        with lock:

            forecast_cache[key] = {
                "time": now,
                "data": response
            }

        return response

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/weather")
def weather(
    lat: float = Query(...),
    lon: float = Query(...)
):

    data = fetch_weather_logic(lat, lon)

    if data:
        return {
            "status": "success",
            "weather": data
        }

    return {
        "status": "error"
    }

@app.get("/routes")
def routes():

    return [route.path for route in app.routes]

# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    init_db()

    threading.Thread(
        target=load_hycom,
        daemon=True
    ).start()
