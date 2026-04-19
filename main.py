from fastapi import FastAPI, Query
import xarray as xr
import numpy as np

app = FastAPI()

DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

# 🔥 起動時に1回だけ読み込み（日本周辺だけ）
ds = xr.open_dataset(
    DATA_URL,
    engine="netcdf4",
    decode_times=False
).sel(
    lat=slice(30, 46),     # 日本全体
    lon=slice(129, 146)
)

@app.get("/current")
def get_current(lat: float = Query(...), lon: float = Query(...)):
    try:
        subset = ds.sel(
            lat=lat,
            lon=lon,
            method="nearest"
        ).isel(time=0)

        u = subset["water_u"].values.flatten()[0]
        v = subset["water_v"].values.flatten()[0]

        # 陸チェック
        if np.isnan(u):
            return {"status": "error", "message": "陸地です"}

        speed = np.sqrt(u**2 + v**2) * 1.94384

        return {
            "status": "success",
            "velocity_knot": round(float(speed), 2),
            "lat": lat,
            "lon": lon
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}
