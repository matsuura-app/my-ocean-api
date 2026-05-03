from fastapi import FastAPI, Query
import xarray as xr
import numpy as np

app = FastAPI()

DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

# 🔥 起動時に1回だけ読み込み（日本周辺）
ds = xr.open_dataset(
    DATA_URL,
    engine="netcdf4",
    decode_times=False
).sel(
    lat=slice(30, 46),
    lon=slice(129, 146)
)

# =========================
# HYCOM
# =========================
def get_from_hycom(lat, lon):
    try:
        subset = ds.sel(
            lat=lat,
            lon=lon,
            method="nearest"
        ).isel(time=0)

        # 深さがある場合だけ0層取得
        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values.flatten()[0])
        v = float(subset["water_v"].values.flatten()[0])

        # 陸チェック
        if np.isnan(u):
            return {"status": "error", "message": "land"}

        # 流速（knot）
        speed = np.sqrt(u**2 + v**2) * 1.94384

        # 流向（0〜360）
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
# メインAPI
# =========================
@app.get("/current")
def get_current(lat: float = Query(...), lon: float = Query(...)):
    return get_from_hycom(lat, lon)


# Render用
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

from datetime import datetime, timedelta

@app.get("/forecast")
def get_forecast(lat: float = Query(...), lon: float = Query(...)):

    try:
        results = []

        for h in range(48):

            subset = ds.sel(
                lat=lat,
                lon=lon,
                method="nearest"
            ).isel(time=h)

            # 深さ対応
            if "depth" in subset.dims:
                subset = subset.isel(depth=0)

            u = float(subset["water_u"].values.flatten()[0])
            v = float(subset["water_v"].values.flatten()[0])

            if np.isnan(u):
                continue

            speed = np.sqrt(u**2 + v**2) * 1.94384
            direction = (np.degrees(np.arctan2(v, u)) + 360) % 360

            results.append({
                "time": h,  # ←シンプルに時間インデックス
                "speed": round(speed, 2),
                "direction": round(direction, 1)
            })

        return {
            "status": "success",
            "data": results
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
import requests
from datetime import datetime, timedelta

@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str):

    api_key = "75582c7dd45041e7990dcc058ffa60b7"

    result = []

    now = datetime.utcnow()

    for hour in range(48):

        target = now + timedelta(hours=hour)

        time_string = target.strftime("%Y%m%d%H%M")

        url = (
            f"https://api.msil.go.jp/"
            f"tidal-current-prediction/v3/data"
            f"?areaCode={areaCode}"
            f"&time={time_string}"
            f"&key={api_key}"
        )

        try:

            response = requests.get(url, timeout=10)

            json_data = response.json()

            features = json_data.get("features", [])

            if not features:
                continue

            props = features[0]["properties"]

            result.append({
                "time": hour,
                "speed": props.get("currentSpeedKt", 0.0),
                "direction": props.get("currentDirection", 0.0)
            })

        except Exception as e:
            print("ERROR:", e)

    return {
        "status": "success",
        "data": result
    }
