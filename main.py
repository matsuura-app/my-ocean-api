from fastapi import FastAPI, Query
import xarray as xr
import numpy as np

app = FastAPI()

DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

# 🔥 起動時に1回だけ読み込む（超重要）
ds = xr.open_dataset(
    DATA_URL,
    engine="netcdf4",
    decode_times=False
).sel(
    lat=slice(33, 35),   # ←呉周辺だけに絞る（重要）
    lon=slice(131, 134)
)

@app.get("/current")
def get_current(lat: float = Query(...), lon: float = Query(...)):
    try:
        subset = ds.sel(
            lat=lat,
            lon=lon,
            depth=0,
            method="nearest"
        ).isel(time=0)

        u = float(subset["water_u"].values)
        v = float(subset["water_v"].values)

        speed = np.sqrt(u**2 + v**2) * 1.94384

        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "lat": lat,
            "lon": lon
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
