import os
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import time
import copy

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
# LOCK / CACHE
# =========================================================

lock = threading.Lock()

umishiru_cache = {}
forecast_cache = {}

umishiru_refreshing = set()
forecast_refreshing = set()

CACHE_TTL = 21600  # 6h

# =========================================================
# HYCOM
# =========================================================

DATA_URL = (
    "https://tds.hycom.org/thredds/dodsC/"
    "FMRC_ESPC-D-V02_uv3z/"
    "FMRC_ESPC-D-V02_uv3z_best.ncd"
)

ds_local = None
hycom_ready = False


# =========================================================
# DAILY RESET
# =========================================================

def reset_daily_cache():
    global umishiru_cache, forecast_cache

    last_reset_day = None

    while True:
        now = datetime.now(JST)

        if last_reset_day != now.date():
            with lock:
                print("🔄 Daily cache reset", flush=True)
                umishiru_cache = {}
                forecast_cache = {}

            last_reset_day = now.date()

        time.sleep(60)


# =========================================================
# HYCOM LOAD（統一 decode_times=False）
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

        with lock:
            ds_local = ds
            hycom_ready = True

        print("HYCOM loaded", flush=True)

    except Exception as e:
        hycom_ready = False
        print(f"HYCOM load error: {e}", flush=True)


# =========================================================
# WATCHDOG（軽量化）
# =========================================================

def hycom_watchdog():
    global ds_local, hycom_ready

    while True:
        try:
            test = xr.open_dataset(
                DATA_URL,
                engine="netcdf4",
                decode_times=False
            )

            new_time_size = test.sizes["time"]
            test.close()

            if ds_local is None:
                print("HYCOM first load from watchdog", flush=True)

                new_ds = xr.open_dataset(
                    DATA_URL,
                    engine="netcdf4",
                    decode_times=False
                ).sel(lat=slice(30, 46), lon=slice(129, 146))

                with lock:
                    ds_local = new_ds
                    hycom_ready = True

            else:
                old_time_size = ds_local.sizes["time"]

                if new_time_size != old_time_size:
                    print("🔄 HYCOM updated", flush=True)

                    new_ds = xr.open_dataset(
                        DATA_URL,
                        engine="netcdf4",
                        decode_times=False
                    ).sel(lat=slice(30, 46), lon=slice(129, 146))

                    with lock:
                        old = ds_local
                        ds_local = new_ds
                        hycom_ready = True

                    if old:
                        try:
                            old.close()
                        except:
                            pass

        except Exception as e:
            print(f"HYCOM not reachable: {e}", flush=True)

        time.sleep(12 * 3600)


# =========================================================
# CURRENT
# =========================================================

def get_from_hycom(lat, lon):

    if ds_local is None:
        return {"status": "loading"}

    try:
        subset = ds_local.sel(lat=lat, lon=lon, method="nearest")

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values)
        v = float(subset["water_v"].values)

        if np.isnan(u) or np.isnan(v):
            return {"status": "error", "message": "land"}

        speed = np.sqrt(u**2 + v**2) * 1.94384
        direction = (np.degrees(np.arctan2(v, u)) + 360) % 360

        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "direction": round(direction, 1),
            "source": "HYCOM"
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


# =========================================================
# FORECAST（time修正済み）
# =========================================================

def build_forecast_response(lat, lon):

    subset = ds_local.sel(lat=lat, lon=lon, method="nearest")

    results = []
    max_time = min(48, subset.sizes["time"])

    time_values = subset["time"].values

    for h in range(max_time):
        try:
            data = subset.isel(time=h)

            if "depth" in data.dims:
                data = data.isel(depth=0)

            u = float(data["water_u"].values)
            v = float(data["water_v"].values)

            if np.isnan(u) or np.isnan(v):
                continue

            speed = np.sqrt(u**2 + v**2) * 1.94384
            direction = (np.degrees(np.arctan2(v, u)) + 360) % 360

            results.append({
                "hour": h,
                "time_index": h,
                "model_time": str(time_values[h]),
                "speed": round(speed, 2),
                "direction": round(direction, 1)
            })
        except:
            continue

    return {"status": "success", "data": results}


# =========================================================
# WEATHER
# =========================================================

def fetch_weather_logic(lat, lon):

    try:
        r = session.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m,windspeed_10m,winddirection_10m,weathercode",
                "forecast_days": 7,
                "timezone": "Asia/Tokyo"
            },
            timeout=10
        )

        if r.status_code == 200:
            return r.json()

    except:
        pass

    return None


# =========================================================
# UMISHIRU（そのまま安定版）
# =========================================================

def fetch_umishiru_hour(area_code, hour_offset):

    try:
        base = datetime.now(JST).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        target = base + timedelta(hours=hour_offset)

        r = session.get(
            "https://api.msil.go.jp/tidal-current-prediction/v3/data",
            params={
                "areaCode": area_code,
                "time": target.strftime("%Y%m%d%H%M"),
                "key": API_KEY
            },
            timeout=20
        )

        if r.status_code != 200:
            return None

        data = r.json()
        features = data.get("features", [])

        if not features:
            return None

        p = features[0].get("properties", {})

        return {
            "time": hour_offset,
            "speed": float(p.get("currentSpeedKt", 0) or 0),
            "direction": float(p.get("currentDirection", 0) or 0)
        }

    except:
        return None


def fetch_48h_parallel(area_code):

    with ThreadPoolExecutor(max_workers=8) as ex:
        res = list(ex.map(lambda h: fetch_umishiru_hour(area_code, h), range(48)))

    res = [r for r in res if r]
    res.sort(key=lambda x: x["time"])

    return {"status": "success", "data": res} if res else {"status": "error", "data": []}


# =========================================================
# API
# =========================================================

@app.get("/")
def root():
    return {"status": "ok", "hycom_ready": hycom_ready}

@app.get("/umishiru_forecast")
def umishiru_forecast(
    areaCode: str = Query(..., alias="areaCode")
):

    if not API_KEY:
        return {"status": "error", "message": "MSIL_API_KEY missing"}

    data = fetch_48h_parallel(areaCode)

    return {
    "status": "success",
    "data": data["data"]
}
    
@app.get("/current")
def current(lat: float = Query(...), lon: float = Query(...)):
    return get_from_hycom(lat, lon)


@app.get("/forecast")
def forecast(lat: float, lon: float):

    if not hycom_ready or ds_local is None:
        return {"status": "loading", "data": []}

    key = f"{round(lat,2)}_{round(lon,2)}"
    now = datetime.utcnow().timestamp()

    with lock:
        cache = forecast_cache.get(key)

    if cache:
        if now - cache["time"] > CACHE_TTL:

            with lock:
                if key not in forecast_refreshing:
                    forecast_refreshing.add(key)

            def refresh():
                try:
                    new = build_forecast_response(lat, lon)

                    if new["status"] == "success":
                        with lock:
                            forecast_cache[key] = {
                                "time": datetime.utcnow().timestamp(),
                                "data": new
                            }
                finally:
                    with lock:
                        forecast_refreshing.discard(key)

            threading.Thread(target=refresh, daemon=True).start()

        return copy.deepcopy(cache["data"])

    try:
        res = build_forecast_response(lat, lon)

        with lock:
            forecast_cache[key] = {
                "time": now,
                "data": res
            }

        return res

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/weather")
def weather(lat: float, lon: float):
    data = fetch_weather_logic(lat, lon)
    return {"status": "success", "weather": data} if data else {"status": "error"}


@app.get("/routes")
def routes():
    return [r.path for r in app.routes]


# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    threading.Thread(target=load_hycom, daemon=True).start()
    threading.Thread(target=hycom_watchdog, daemon=True).start()
    threading.Thread(target=reset_daily_cache, daemon=True).start()
