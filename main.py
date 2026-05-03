from fastapi import FastAPI, Query
import xarray as xr
import numpy as np
import requests

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import threading

app = FastAPI()

DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

# =========================
# HYCOMロード
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
# ウォームアップ（重要）
# =========================
def warmup_all():

    try:
        print("WARMING UP HYCOM...")

        _ = ds.isel(time=0, lat=0, lon=0).values

        print("HYCOM READY")

    except Exception as e:
        print("HYCOM WARMUP FAILED:", e)

# =========================
# 起動時バックグラウンドウォーム
# =========================
@app.on_event("startup")
def startup_event():

    thread = threading.Thread(target=warmup_all)
    thread.start()

# =========================
# HYCOM 現在流
# =========================
def get_from_hycom(lat, lon):

    try:

        subset = ds.sel(lat=lat, lon=lon, method="nearest")
        subset = subset.isel(time=0)

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = subset["water_u"].values
        v = subset["water_v"].values

        if np.isnan(u) or np.isnan(v):
            return {"status": "error", "message": "land"}

        u = float(u)
        v = float(v)

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
# current
# =========================
@app.get("/current")
def get_current(lat: float = Query(...), lon: float = Query(...)):
    return get_from_hycom(lat, lon)

# =========================
# warmup API
# =========================
@app.get("/warmup")
def warmup():
    warmup_all()
    return {"status": "warmed"}

# =========================
# forecast
# =========================
@app.get("/forecast")
def get_forecast(lat: float = Query(...), lon: float = Query(...)):

    key = f"{round(lat,2)}_{round(lon,2)}"
    now = datetime.utcnow().timestamp()

    if key in forecast_cache:
        if now - forecast_cache[key]["time"] < 1800:
            return forecast_cache[key]["data"]

    try:

        results = []

        for h in range(48):

            subset = ds.sel(lat=lat, lon=lon, method="nearest").isel(time=h)

            if "depth" in subset.dims:
                subset = subset.isel(depth=0)

            u = subset["water_u"].values
            v = subset["water_v"].values

            if np.isnan(u) or np.isnan(v):
                continue

            speed = np.sqrt(float(u)**2 + float(v)**2) * 1.94384
            direction = (np.degrees(np.arctan2(float(v), float(u))) + 360) % 360

            results.append({
                "time": h,
                "speed": round(speed, 2),
                "direction": round(direction, 1)
            })

        response = {
            "status": "success",
            "data": results
        }

        forecast_cache[key] = {
            "time": now,
            "data": response
        }

        return response

    except Exception as e:
        return {"status": "error", "message": str(e)}

# =========================
# 海しる
# =========================
API_KEY = "YOUR_KEY"

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
# 海しる forecast
# =========================
@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str):

    now = datetime.utcnow().timestamp()

    if areaCode in umishiru_cache:
        if now - umishiru_cache[areaCode]["time"] < 300:
            return umishiru_cache[areaCode]["data"]

    with ThreadPoolExecutor(max_workers=8) as executor:

        results = list(executor.map(
            lambda h: fetch_umishiru_hour(areaCode, h),
            range(48)
        ))

    data = [r for r in results if r]

    response = {
        "status": "success",
        "data": data
    }

    umishiru_cache[areaCode] = {
        "time": now,
        "data": response
    }

    return response

# =========================
# run
# =========================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
