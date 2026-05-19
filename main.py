from fastapi import FastAPI, Query
import xarray as xr
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from datetime import timezone
import threading
import sqlite3

app = FastAPI()

# =========================
# SQLite
# =========================
tide_conn = sqlite3.connect(
    "tides.db",
    check_same_thread=False
)

tide_conn.row_factory = sqlite3.Row
cur = tide_conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS tides (
    point TEXT,
    datetime TEXT,
    height REAL,
    PRIMARY KEY(point, datetime)
)
""")

tide_conn.commit()
# =========================
# HYCOM設定
# =========================
DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

ds_local = None
hycom_ready = False

def load_hycom():
    global ds_local, hycom_ready

    print("HYCOM loading...")

    ds_local = xr.open_dataset(
        DATA_URL,
        engine="netcdf4",
        decode_times=False
    ).sel(
        lat=slice(30, 46),
        lon=slice(129, 146)
    )

    hycom_ready = True
    print("HYCOM ready")


# =========================
# キャッシュ
# =========================
forecast_cache = {}
umishiru_cache = {}
lock = threading.Lock()

CACHE_TTL = 1800  # 30分


# =========================
# 起動処理
# =========================
# =========================
# 起動処理
# =========================
@app.on_event("startup")
def startup():

    # SQLite保存テスト
    save_tide(
        "呉",
        "2026-05-19 12:00:00",
        2.31
    )

    # HYCOM読み込み
    threading.Thread(
        target=load_hycom,
        daemon=True
    ).start()

    # 海しるウォームアップ
    try:
        threading.Thread(
            target=update_umishiru_background,
            args=("default",),
            daemon=True
        ).start()

    except Exception as e:
        print("warmup failed:", e)
# =========================
# HYCOM現在流
# =========================
def get_from_hycom(lat, lon):

    if not hycom_ready or ds_local is None:
        return {"status": "loading", "message": "HYCOM initializing"}

    try:
        subset = ds_local.sel(lat=lat, lon=lon, method="nearest").isel(time=0)

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


# =========================
# HYCOM forecast
# =========================
@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...)):

    if not hycom_ready or ds_local is None:
        return {"status": "loading", "data": []}

    key = f"{round(lat,2)}_{round(lon,2)}"
    now = datetime.utcnow().timestamp()

    if key in forecast_cache:
        cached = forecast_cache[key]
        if now - cached["time"] < CACHE_TTL:
            return cached["data"]

    point = ds_local.sel(lat=lat, lon=lon, method="nearest")

    results = []

    # =========================
    # 今日0時UTC
    # =========================
    utc_now = datetime.utcnow()

    base_utc = utc_now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    # HYCOM開始時刻
    hycom_start = datetime(2000, 1, 1)

    # HYCOM time[0]
    first_time = float(ds_local["time"].values[0])

    first_datetime = hycom_start + timedelta(hours=first_time)

    # 0時との差
    offset_hours = int(
        (base_utc - first_datetime).total_seconds() / 3600
    )

    # =========================
    # 48時間
    # =========================
    for h in range(48):

        idx = offset_hours + h

        subset = point.isel(time=idx)

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values)
        v = float(subset["water_v"].values)

        if np.isnan(u) or np.isnan(v):
            results.append({
                "time": h,
                "speed": 0.0,
                "direction": 0.0
            })
            continue

        results.append({
            "time": h,
            "speed": round(np.sqrt(u**2 + v**2) * 1.94384, 2),
            "direction": round((np.degrees(np.arctan2(v, u)) + 360) % 360, 1)
        })

    response = {
        "status": "success",
        "data": results
    }

    # キャッシュ整理
    with lock:
        if len(forecast_cache) > 200:
            while len(forecast_cache) > 150:
                forecast_cache.pop(next(iter(forecast_cache)), None)

    forecast_cache[key] = {
        "time": now,
        "data": response
    }

    return response

# =========================
# Tide Save
# =========================
def save_tide(point, dt, height):

    try:

        cur = tide_conn.cursor()

        cur.execute(
            """
            INSERT OR REPLACE INTO tides
            (point, datetime, height)
            VALUES (?, ?, ?)
            """,
            (
                point,
                dt,
                height
            )
        )

        tide_conn.commit()

    except Exception as e:
        print("save_tide error:", e)
# =========================
# 海しるAPI
# =========================
API_KEY = "75582c7dd45041e7990dcc058ffa60b7"

def fetch_umishiru_hour(area_code, hour):

    try:
        # 今日0時UTC
        base = datetime.utcnow().replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        target = base + timedelta(hours=hour)

        time_string = target.strftime("%Y%m%d%H%M")

        url = (
            "https://api.msil.go.jp/tidal-current-prediction/v3/data"
            f"?areaCode={area_code}"
            f"&time={time_string}"
            f"&key={API_KEY}"
        )

        r = requests.get(url, timeout=15)

        if r.status_code != 200:
            return None

        data = r.json()
        features = data.get("features", [])

        if not features:
            return None

        p = features[0]["properties"]

        return {
            "time": hour,
            "speed": p.get("currentSpeedKt", 0.0),
            "direction": p.get("currentDirection", 0.0)
        }

    except Exception as e:
        print("umishiru error:", e)
        return None


def fetch_48h(area_code):

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(
            lambda h: fetch_umishiru_hour(area_code, h),
            range(48)
        ))

    filtered = [r for r in results if r]

    if not filtered:
        return {"status": "error", "data": []}

    return {"status": "success", "data": filtered}


# =========================
# 海しるバックグラウンド更新
# =========================
def update_umishiru_background(areaCode):

    cache = umishiru_cache.setdefault(areaCode, {
        "last_good": None,
        "date": None,
        "updating": False,
        "building": False
    })

    try:
        data = fetch_48h(areaCode)

        if data["status"] == "success":
            cache["last_good"] = data
            cache["date"] = datetime.utcnow().date()

    finally:
        with lock:
            cache["updating"] = False
# =========================
# 海しる取得（即レス + 裏更新）
# =========================
def get_umishiru(areaCode):

    cache = umishiru_cache.setdefault(areaCode, {
        "last_good": None,
        "updating": False,
        "date": None,
        "building": False
    })

    # =========================
    # 1. 即レス（キャッシュあり）
    # =========================
    if cache["last_good"]:

        with lock:
            if cache.get("updating"):
                return cache["last_good"]

            cache["updating"] = True

        threading.Thread(
            target=update_umishiru_background,
            args=(areaCode,),
            daemon=True
        ).start()

        return cache["last_good"]

    # =========================
    # 2. 初回取得
    # =========================
    cache["building"] = True

    try:
        data = fetch_48h(areaCode)

        if data["status"] == "success":
            cache["last_good"] = data

        return data

    finally:
        cache["building"] = False
# =========================
# API
# =========================
@app.get("/current")
def current(lat: float = Query(...), lon: float = Query(...)):
    return get_from_hycom(lat, lon)

@app.get("/tide")
def get_tide(
    point: str,
    date: str
):

    try:

        cur = tide_conn.cursor()

        start = f"{date} 00:00:00"
        end = f"{date} 23:59:59"

        rows = cur.execute(
            """
            SELECT datetime, height
            FROM tides
            WHERE point = ?
            AND datetime BETWEEN ? AND ?
            ORDER BY datetime
            """,
            (point, start, end)
        ).fetchall()

        result = []

        for r in rows:

            result.append({
                "time": r["datetime"],
                "height": r["height"]
            })

        return {
            "status": "success",
            "data": result
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str):
    return get_umishiru(areaCode)


# =========================
# 起動
# =========================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
