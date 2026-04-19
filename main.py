from fastapi import FastAPI, Query
import xarray as xr
import numpy as np

app = FastAPI()

DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

# 🔥 起動時に1回だけ読み込み（高速化）
ds = xr.open_dataset(
    DATA_URL,
    engine="netcdf4",
    decode_times=False
).sel(
    lat=slice(30, 46),
    lon=slice(129, 146)
)

# 🔧 HYCOM取得関数（分離しておく）
def get_from_hycom(lat, lon):
    try:
        subset = ds.sel(
            lat=lat,
            lon=lon,
            method="nearest"
        ).isel(time=0)

        # depthがある場合だけ処理
        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values.flatten()[0])
        v = float(subset["water_v"].values.flatten()[0])

        # 陸チェック
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


# 🔧 うみしる（仮：あとで実装）
def get_from_umishiru(lat, lon):
    return {
        "status": "error",
        "message": "umishiru not implemented"
    }


# 🔥 メインAPI（ここが完成形）
@app.get("/current")
def get_current(lat: float = Query(...), lon: float = Query(...)):

    # ① HYCOMで取得
    result = get_from_hycom(lat, lon)

    # ② ダメならうみしる（将来）
    if result["status"] == "error":
        return get_from_umishiru(lat, lon)

    # ③ 成功ならそのまま返す
    return result
