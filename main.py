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
# CACHE & LOCK
# =========================================================

lock = threading.Lock()

umishiru_cache = {}
forecast_cache = {}

CACHE_TTL = 1800  # HYCOM / forecast

last_hycom_signature = None

# =========================================================
# TIME UTIL
# =========================================================
import time

JST = timezone(timedelta(hours=9))


def is_new_day(last_update: datetime):
    now = datetime.now(JST)
    return now.date() != last_update.date()


# =========================================================
# DAILY RESET (核心)
# =========================================================
def reset_daily_cache():
    global umishiru_cache, forecast_cache

    last_reset_day = None

    while True:
        now = datetime.now(JST)

        if last_reset_day != now.date():
            with lock:
                print("🔄 Daily cache reset")
                umishiru_cache = {}
                forecast_cache = {}

            last_reset_day = now.date()

        time.sleep(60)
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
        ds = xr.open_dataset(
            DATA_URL,
            engine="netcdf4",
            decode_times=False
        ).sel(
            lat=slice(30, 46),
            lon=slice(129, 146)
        )
        ds_local = ds
        hycom_ready = True
        print("HYCOM loaded", flush=True)
    except Exception as e:
        hycom_ready = False
        print("HYCOM load error:", e, flush=True)
# =========================================================
# HYCOM WATCHDOG（追加）
# =========================================================
def hycom_watchdog():
    global ds_local, hycom_ready
    while True:
        try:
            test_ds = xr.open_dataset(
                DATA_URL,
                decode_times=False,
                chunks={}
            )
            new_time_size = test_ds.sizes.get("time", 0)

            if ds_local is not None:
                old_time_size = ds_local.sizes.get("time", 0)

                if new_time_size != old_time_size:
                    print("🔄 HYCOM updated", flush=True)
                    new_ds = xr.open_dataset(
                        DATA_URL,
                        engine="netcdf4",
                        decode_times=False
                    ).sel(
                        lat=slice(30, 46),
                        lon=slice(129, 146)
                    )

                    old = ds_local
                    ds_local = new_ds

                    hycom_ready = True
                    try:
                        old.close()
                    except:
                        pass
            test_ds.close()
        except Exception as e:
            print("HYCOM not reachable", flush=True)
        # 12時間ごと
        time.sleep(12 * 3600)
# =========================================================
# HYCOM CURRENT
# =========================================================

def get_from_hycom(lat, lon):

    if ds_local is None:
        return {
            "status": "loading",
            "message": "HYCOM initializing"
        }

    try:

        subset = ds_local.sel(
            lat=slice(lat - 0.2, lat + 0.2),
            lon=slice(lon - 0.2, lon + 0.2)
        )

        # time=0 取得時にFMRC壊れてると落ちる
        try:
            subset = subset.isel(time=0)
        except Exception as e:
            return {
                "status": "error",
                "message": f"HYCOM time access error: {str(e)}"
            }

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

    except Exception as e:
        print("weather fetch error:", e)
    return None
# =========================================================
# UMISHIRU
# =========================================================

def fetch_umishiru_hour(area_code, hour_offset):

    try:

        base_jst = datetime.now(JST).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        target = base_jst + timedelta(hours=hour_offset)

        time_string = target.strftime("%Y%m%d%H%M")

        url = "https://api.msil.go.jp/tidal-current-prediction/v3/data"

        params = {
            "areaCode": area_code,
            "time": time_string,
            "key": API_KEY
        }

        r = session.get(
            url,
            params=params,
            timeout=10
        )

        if r.status_code != 200:
            return None

        data = r.json()

        features = data.get("features", [])

        if not features:
            return None

        p = features[0].get("properties", {})

        speed = float(
            p.get("currentSpeedKt", 0.0) or 0.0
        )

        direction = float(
            p.get("currentDirection", 0.0) or 0.0
        )

        return {
            "time": hour_offset,
            "speed": speed,
            "direction": direction
        }

    except:
        return None


def fetch_48h_parallel(area_code):

    with ThreadPoolExecutor(max_workers=8) as executor:

        results = list(
            executor.map(
                lambda h: fetch_umishiru_hour(area_code, h),
                range(48)
            )
        )

    filtered = [r for r in results if r]

    filtered.sort(key=lambda x: x["time"])

    if not filtered:

        return {
            "status": "error",
            "data": []
        }

    return {
        "status": "success",
        "data": filtered
    }


@app.get("/umishiru_forecast")
def umishiru_forecast(
    areaCode: str = Query(..., alias="areaCode")
):

    if not API_KEY:
        return {
            "status": "error",
            "message": "MSIL_API_KEY missing"
        }

    now_jst = datetime.now(JST)

    with lock:
        cache = umishiru_cache.get(areaCode)

    # =====================================================
    # キャッシュ存在時
    # =====================================================

    if cache:

        # まず古いデータでも即返す
        cached_data = cache["data"]

        # 期限切れなら裏更新
        if cache["expires"] <= now_jst:

            def refresh():

                try:

                    new_data = fetch_48h_parallel(areaCode)

                    if new_data["status"] == "success":

                        with lock:

                            umishiru_cache[areaCode] = {
                                "expires": now_jst + timedelta(hours=6),
                                "data": new_data
                            }

                        print(
                            f"Umishiru refreshed: {areaCode}",
                            flush=True
                        )

                except Exception as e:

                    print(
                        f"Umishiru refresh error: {e}",
                        flush=True
                    )

            threading.Thread(
                target=refresh,
                daemon=True
            ).start()

        return cached_data

    # =====================================================
    # 初回取得
    # =====================================================

    data = fetch_48h_parallel(areaCode)

    if data["status"] == "success":

        with lock:

            umishiru_cache[areaCode] = {
                "expires": now_jst + timedelta(hours=6),
                "data": data
            }

    return data
# =========================================================
# API
# =========================================================

@app.get("/")
def root():
    return {
        "status": "ok",
        "server": "marine-final",
        "hycom_ready": hycom_ready
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

            try:
                data = subset.isel(time=h)
            except Exception:
                continue

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
def weather(lat: float = Query(...), lon: float = Query(...)):

    data = fetch_weather_logic(lat, lon)

    if data is not None and "hourly" in data:
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
    # HYCOM初回ロード
    threading.Thread(
        target=load_hycom,
        daemon=True
    ).start()
    # HYCOM監視
    threading.Thread(
        target=hycom_watchdog,
        daemon=True
    ).start()
    # 日跨ぎリセット
    threading.Thread(
        target=reset_daily_cache,
        daemon=True
    ).start()
