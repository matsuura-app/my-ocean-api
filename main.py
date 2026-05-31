import os
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import time

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

# キャッシュ
umishiru_cache = {}
forecast_cache = {}

# 更新中管理
umishiru_refreshing = set()
forecast_refreshing = set()

# 6時間
CACHE_TTL = 21600

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
# DAILY RESET
# =========================================================

def reset_daily_cache():

    global umishiru_cache
    global forecast_cache

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
# HYCOM LOAD
# =========================================================
def load_hycom():
    global ds_local
    global hycom_ready
    print("HYCOM loading...", flush=True)
    try:
        ds = xr.open_dataset(
            DATA_URL,
            engine="netcdf4",
            decode_times=False,
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
        print(
            f"HYCOM load error: {e}",
            flush=True
        )
# =========================================================
# HYCOM WATCHDOG
# =========================================================
def hycom_watchdog():
    global ds_local
    global hycom_ready
    while True:
        test_ds = None
        old_ds = None
        try:
            test_ds = xr.open_dataset(
                DATA_URL,
                engine="netcdf4",
                decode_times=False
            )

            new_time_size = test_ds.sizes.get(
                "time",
                0
            )
            # 初回復旧
            if ds_local is None:
                print(
                    "HYCOM first load from watchdog",
                    flush=True
                )
                new_ds = xr.open_dataset(
                    DATA_URL,
                    engine="netcdf4",
                    decode_times=False,
                ).sel(
                    lat=slice(30, 46),
                    lon=slice(129, 146)
                )
                with lock:
                    ds_local = new_ds
                    hycom_ready = True
            # 更新検知
            else:
                old_time_size = ds_local.sizes.get(
                    "time",
                    0
                )
                if new_time_size != old_time_size:
                    print(
                        "🔄 HYCOM updated",
                        flush=True
                    )
                    new_ds = xr.open_dataset(
                        DATA_URL,
                        engine="netcdf4",
                        decode_times=False,
                        chunks={}
                    ).sel(
                        lat=slice(30, 46),
                        lon=slice(129, 146)
                    )
                    with lock:
                        old_ds = ds_local
                        ds_local = new_ds
                        hycom_ready = True
                    time.sleep(5)
                    if old_ds is not None:
                        try:
                            old_ds.close()
                        except Exception:
                            pass

        except Exception as e:
            print(
                f"HYCOM not reachable: {e}",
                flush=True
            )

        finally:

            if test_ds is not None:

                try:
                    test_ds.close()
                except Exception:
                    pass

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
            lat=lat,
            lon=lon,
            method="nearest"
        )

        subset = subset.isel(time=0)

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values)
        v = float(subset["water_v"].values)

        if np.isnan(u) or np.isnan(v):

            return {
                "status": "error",
                "message": "land"
            }

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
# FORECAST BUILDER
# =========================================================
def build_forecast_response(lat, lon):

    subset = ds_local.sel(
        lat=lat,
        lon=lon,
        method="nearest"
    )
    time_values = subset["time"].values

    print(time_values[:20], flush=True)
    results = []
    
    # =========================
    # ★ 現在時刻インデックス
    # =========================
    max_time = min(48, subset.sizes["time"])
    base_time = datetime.utcnow()
    
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

            hour_offset = float(time_values[h])

            results.append({
                "hour_offset": hour_offset,
                "estimated_time": (
                    base_time + timedelta(hours=hour_offset)
                ).isoformat(),
                "speed": round(speed, 2),
                "direction": round(direction, 1)
            })

        except:
            continue

    return {
        "status": "success",
        "data": results
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

        print(
            f"weather fetch error: {e}",
            flush=True
        )

    return None

# =========================================================
# UMISHIRU
# =========================================================

def fetch_umishiru_hour(
    area_code,
    hour_offset
):

    try:

        base_jst = datetime.now(JST).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        target = base_jst + timedelta(
            hours=hour_offset
        )

        time_string = target.strftime(
            "%Y%m%d%H%M"
        )

        url = (
            "https://api.msil.go.jp/"
            "tidal-current-prediction/v3/data"
        )

        params = {
            "areaCode": area_code,
            "time": time_string,
            "key": API_KEY
        }

        r = session.get(
            url,
            params=params,
            timeout=20
        )

        if r.status_code != 200:
            return None

        data = r.json()

        features = data.get(
            "features",
            []
        )

        if not features:
            return None

        p = features[0].get(
            "properties",
            {}
        )

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

    except Exception as e:

        print(
            f"umishiru fetch error: {e}",
            flush=True
        )

        return None

def fetch_48h_parallel(area_code):

    with ThreadPoolExecutor(
        max_workers=8
    ) as executor:

        results = list(
            executor.map(
                lambda h: fetch_umishiru_hour(
                    area_code,
                    h
                ),
                range(48)
            )
        )

    filtered = [
        r for r in results if r
    ]

    filtered.sort(
        key=lambda x: x["time"]
    )

    if not filtered:

        return {
            "status": "error",
            "data": []
        }

    return {
        "status": "success",
        "data": filtered
    }

# =========================================================
# UMISHIRU API
# =========================================================

@app.get("/umishiru_forecast")
def umishiru_forecast(
    areaCode: str = Query(
        ...,
        alias="areaCode"
    )
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
        print(
            f"UMISHIRU CACHE HIT: {areaCode}",
            flush=True
        )
        cached_data = cache["data"]

        # TTL切れなら裏更新
        if cache["expires"] <= now_jst:

            with lock:

                already_refreshing = (
                    areaCode in umishiru_refreshing
                )

                if not already_refreshing:
                    umishiru_refreshing.add(areaCode)

            if not already_refreshing:

                def refresh():

                    try:

                        new_data = fetch_48h_parallel(
                            areaCode
                        )

                        if new_data["status"] == "success":

                            with lock:

                                umishiru_cache[areaCode] = {
                                    "expires": (
                                        datetime.now(JST)
                                        + timedelta(hours=6)
                                    ),
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

                    finally:

                        with lock:
                            umishiru_refreshing.discard(areaCode)

                threading.Thread(
                    target=refresh,
                    daemon=True
                ).start()

        return cached_data

    # =====================================================
    # 初回取得
    # =====================================================
    print(
        f"UMISHIRU CACHE MISS: {areaCode}",
        flush=True
    )
    data = fetch_48h_parallel(areaCode)

    if data["status"] == "success":

        with lock:

            umishiru_cache[areaCode] = {
                "expires": (
                    now_jst
                    + timedelta(hours=6)
                ),
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

    # =====================================================
    # キャッシュ存在時
    # =====================================================

    if cache:

        cached_data = cache["data"]

        # TTL切れなら裏更新
        if now - cache["time"] >= CACHE_TTL:

            with lock:

                already_refreshing = (
                    key in forecast_refreshing
                )

                if not already_refreshing:
                    forecast_refreshing.add(key)

            if not already_refreshing:

                def refresh():

                    try:

                        new_response = build_forecast_response(
                            lat,
                            lon
                        )

                        if new_response["status"] == "success":

                            with lock:

                                forecast_cache[key] = {
                                    "time": datetime.utcnow().timestamp(),
                                    "data": new_response
                                }

                            print(
                                f"Forecast refreshed: {key}",
                                flush=True
                            )

                    except Exception as e:

                        print(
                            f"Forecast refresh error: {e}",
                            flush=True
                        )

                    finally:

                        with lock:
                            forecast_refreshing.discard(key)

                threading.Thread(
                    target=refresh,
                    daemon=True
                ).start()

        return cached_data

    # =====================================================
    # 初回取得
    # =====================================================

    try:

        response = build_forecast_response(
            lat,
            lon
        )

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

    return [
        route.path
        for route in app.routes
    ]

# =========================================================
# STARTUP
# =========================================================
@app.on_event("startup")
def startup():
    print("🚀 Startup begin", flush=True)
    # =====================================================
    # HYCOM初回ロード（リトライ付き・同期）
    # =====================================================
    for i in range(3):
        try:
            print(f"HYCOM load attempt {i+1}", flush=True)
            load_hycom()
            # 成功判定（重要）
            if hycom_ready and ds_local is not None:
                print("✅ HYCOM load success", flush=True)
                break
            else:
                raise Exception("HYCOM not ready after load")
        except Exception as e:
            print(f"⚠️ HYCOM load failed {i+1}: {e}", flush=True)
            time.sleep(5)
    # =====================================================
    # バックグラウンド処理（ここから並列）
    # =====================================================
    threading.Thread(
        target=hycom_watchdog,
        daemon=True
    ).start()
    threading.Thread(
        target=reset_daily_cache,
        daemon=True
    ).start()
    print("✅ Startup complete", flush=True)
