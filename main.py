from fastapi import FastAPI, Query
import xarray as xr
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import threading

app = FastAPI()

# =========================
# HYCOM
# =========================
DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

ds = xr.open_dataset(
    DATA_URL,
    engine="netcdf4",
    decode_times=False
).sel(
    lat=slice(30, 46),
    lon=slice(129, 146)
)

# =========================
# キャッシュ（重要）
# =========================
forecast_cache = {}

umishiru_cache = {
    "date": None,
    "data": None,
    "building": False
}

# =========================
# HYCOM 現在流
# =========================
def get_from_hycom(lat, lon):

    try:
        subset = ds.sel(lat=lat, lon=lon, method="nearest").isel(time=0)

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
# API
# =========================
@app.get("/current")
def current(lat: float = Query(...), lon: float = Query(...)):
    return get_from_hycom(lat, lon)

# =========================
# HYCOM 48h forecast
# =========================
@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...)):

    key = f"{round(lat,2)}_{round(lon,2)}"
    now = datetime.utcnow().timestamp()

    if key in forecast_cache:
        if now - forecast_cache[key]["time"] < 1800:
            return forecast_cache[key]["data"]

    results = []

    for h in range(48):

        subset = ds.sel(
            lat=lat,
            lon=lon,
            method="nearest"
        ).isel(time=h)

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        try:
            u = float(subset["water_u"].values)
            v = float(subset["water_v"].values)
        except:
            continue

        if np.isnan(u) or np.isnan(v):
            continue

        speed = np.sqrt(u**2 + v**2) * 1.94384
        direction = (np.degrees(np.arctan2(v, u)) + 360) % 360

        results.append({
            "time": h,
            "speed": round(speed, 2),
            "direction": round(direction, 1)
        })

    if len(results) == 0:
        return {
            "status": "error",
            "data": []
        }

    response = {
        "status": "success",
        "data": results
    }

    forecast_cache[key] = {
        "time": now,
        "data": response
    }

    return response
# =========================
# 海しるAPIキー
# =========================
API_KEY = "YOUR_KEY"

# =========================
# 海しる 単発取得
# =========================
def fetch_umishiru_hour(area_code, hour):

    try:
        target = datetime.utcnow() + timedelta(hours=hour)
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

    except:
        return None

# =========================
# 48時間取得
# =========================
def fetch_48h(area_code):

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(
            lambda h: fetch_umishiru_hour(area_code, h),
            range(48)
        ))

    return {
        "status": "success",
        "data": [r for r in results if r]
    }

# =========================
# コアロジック（即レス + 裏更新）
# =========================
def get_umishiru(areaCode):

    today = datetime.utcnow().date()

    # ① 今日データあるなら即返す
    if umishiru_cache["date"] == today:
        return umishiru_cache["data"]

    # ② 前日データ返す
    fallback = umishiru_cache["data"]

    # 初回起動時
    if fallback is None:

        # 裏更新開始
        if not umishiru_cache["building"]:

            umishiru_cache["building"] = True

            def build():

                data = fetch_48h(areaCode)

                umishiru_cache["date"] = today
                umishiru_cache["data"] = data
                umishiru_cache["building"] = False

            threading.Thread(target=build).start()

        return {
            "status": "loading",
            "data": []
        }

    # ③ 裏で更新（1回だけ）
    if not umishiru_cache["building"]:

        umishiru_cache["building"] = True

        def build():

            data = fetch_48h(areaCode)

            umishiru_cache["date"] = today
            umishiru_cache["data"] = data
            umishiru_cache["building"] = False

        threading.Thread(target=build).start()

    return fallback
# =========================
# 海しるAPI
# =========================
@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str):
    return get_umishiru(areaCode)

# =========================
# 起動
# =========================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
