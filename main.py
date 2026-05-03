from fastapi import FastAPI, Query
import xarray as xr
import numpy as np
import requests

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

app = FastAPI()

DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

# =========================
# 起動時に1回だけ読み込み
# =========================
ds = xr.open_dataset(
    DATA_URL,
    engine="netcdf4",
    decode_times=False
).sel(
    lat=slice(30, 46),
    lon=slice(129, 146)
)

# =========================
# キャッシュ
# =========================
forecast_cache = {}
umishiru_cache = {}

# =========================
# HYCOM 現在流
# =========================
def get_from_hycom(lat, lon):

    try:

        subset = ds.sel(
            lat=lat,
            lon=lon,
            method="nearest"
        ).isel(time=0)

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values.flatten()[0])
        v = float(subset["water_v"].values.flatten()[0])

        if np.isnan(u):
            return {
                "status": "error",
                "message": "land"
            }

        speed = np.sqrt(u**2 + v**2) * 1.94384
        direction = (np.degrees(np.arctan2(v, u)) + 360) % 360

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

# =========================
# 現在流 API
# =========================
@app.get("/current")
def get_current(
    lat: float = Query(...),
    lon: float = Query(...)
):
    return get_from_hycom(lat, lon)

# =========================
# Warmup
# =========================
@app.get("/warmup")
def warmup():
    return {"status": "ready"}

# =========================
# HYCOM 48時間予測
# =========================
@app.get("/forecast")
def get_forecast(
    lat: float = Query(...),
    lon: float = Query(...)
):

    cache_key = f"{round(lat,2)}_{round(lon,2)}"

    now_time = datetime.utcnow().timestamp()

    # キャッシュ確認
    if cache_key in forecast_cache:

        cache_time = forecast_cache[cache_key]["time"]

        if now_time - cache_time < 1800:

            print("HYCOM CACHE HIT:", cache_key)

            return forecast_cache[cache_key]["data"]

    try:

        results = []

        for h in range(48):

            subset = ds.sel(
                lat=lat,
                lon=lon,
                method="nearest"
            ).isel(time=h)

            if "depth" in subset.dims:
                subset = subset.isel(depth=0)

            u = float(subset["water_u"].values.flatten()[0])
            v = float(subset["water_v"].values.flatten()[0])

            if np.isnan(u):
                continue

            speed = np.sqrt(u**2 + v**2) * 1.94384
            direction = (np.degrees(np.arctan2(v, u)) + 360) % 360

            results.append({
                "time": h,
                "speed": round(speed, 2),
                "direction": round(direction, 1)
            })

        response = {
            "status": "success",
            "data": results
        }

        # キャッシュ保存
        forecast_cache[cache_key] = {
            "time": now_time,
            "data": response
        }

        return response

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

# =========================
# 海しる
# =========================
API_KEY = "75582c7dd45041e7990dcc058ffa60b7"

def fetch_umishiru_hour(area_code, hour):

    target = datetime.utcnow() + timedelta(hours=hour)

    time_string = target.strftime("%Y%m%d%H%M")

    url = (
        f"https://api.msil.go.jp/"
        f"tidal-current-prediction/v3/data"
        f"?areaCode={area_code}"
        f"&time={time_string}"
        f"&key={API_KEY}"
    )

    try:

        response = requests.get(url, timeout=10)

        json_data = response.json()

        features = json_data.get("features", [])

        if not features:
            return None

        props = features[0]["properties"]

        return {
            "time": hour,
            "speed": props.get("currentSpeedKt", 0.0),
            "direction": props.get("currentDirection", 0.0)
        }

    except:
        return None

# =========================
# 海しる48時間予測
# =========================
@app.get("/umishiru_forecast")
def get_umishiru_forecast(areaCode: str):

    now_time = datetime.utcnow().timestamp()

    # キャッシュ確認
    if areaCode in umishiru_cache:

        cache_time = umishiru_cache[areaCode]["time"]

        if now_time - cache_time < 300:

            print("UMISHIRU CACHE HIT:", areaCode)

            return umishiru_cache[areaCode]["data"]

    # 新規取得
    with ThreadPoolExecutor(max_workers=16) as executor:

        results = list(
            executor.map(
                lambda h: fetch_umishiru_hour(areaCode, h),
                range(48)
            )
        )

    data = [r for r in results if r is not None]

    response = {
        "status": "success",
        "data": data
    }

    # キャッシュ保存
    umishiru_cache[areaCode] = {
        "time": now_time,
        "data": response
    }

    return response

# =========================
# Render用
# =========================
if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
