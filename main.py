from fastapi import FastAPI, Query
import xarray as xr
import numpy as np
import requests

app = FastAPI()

DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

# 🔥 起動時に1回だけ（HYCOM）
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

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values.flatten()[0])
        v = float(subset["water_v"].values.flatten()[0])

        if np.isnan(u):
            return {"status": "error", "message": "land"}

        speed = np.sqrt(u**2 + v**2) * 1.94384
        direction = np.degrees(np.arctan2(v, u))

        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "direction": round(direction, 1),
            "source": "HYCOM"
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


# =========================
# うみしる（ここが本体）
# =========================
def get_from_umishiru(lat, lon):
    try:
        # ⚠️ あなたのAPIキーを入れる
        API_KEY = "75582c7dd45041e7990dcc058ffa60b7"

        # ※実際のエンドポイントは契約内容で違うので調整必要
        url = "https://api.umishiru.go.jp/ocean/current"

        params = {
            "lat": lat,
            "lon": lon,
            "apikey": API_KEY
        }

        res = requests.get(url, params=params, timeout=5)

        if res.status_code != 200:
            return {"status": "error", "message": "umishiru api error"}

        data = res.json()

        # ⚠️ ここはAPIのレスポンス形式に合わせて調整
        u = float(data["u"])
        v = float(data["v"])

        speed = np.sqrt(u**2 + v**2) * 1.94384
        direction = np.degrees(np.arctan2(v, u))

        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "direction": round(direction, 1),
            "source": "umishiru"
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


# =========================
# メインAPI
# =========================
@app.get("/current")
def get_current(lat: float, lon: float):
    return {"version": "NEW"}
