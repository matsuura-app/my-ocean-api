from fastapi import FastAPI, Query
import xarray as xr
import numpy as np

app = FastAPI()

# URLが正しいか、Renderからアクセス可能かを確認
DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

@app.get("/current")
def get_current(lat: float = Query(...), lon: float = Query(...)):
    try:
        # 関数内で読み込むように変更（デバッグのため）
        # 本来は外が理想ですが、どこで落ちるか特定します
        ds = xr.open_dataset(
            DATA_URL,
            engine="netcdf4",
            decode_times=False
        ).sel(
            lat=slice(33, 35),
            lon=slice(131, 134)
        )

        # 経度の変換（HYCOM用: 0-360）
        target_lon = lon if lon >= 0 else lon + 360

        subset = ds.sel(lat=lat, lon=target_lon, method="nearest").isel(time=0)
        
        # データの存在確認
        if "water_u" not in subset:
             return {"status": "error", "message": "Variable water_u not found"}

        u = float(subset["water_u"].values.flatten()[0])
        v = float(subset["water_v"].values.flatten()[0])

        if np.isnan(u):
            return {"status": "error", "message": "Selected point is likely on land (NaN)"}

        speed = np.sqrt(u**2 + v**2) * 1.94384

        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "lat": lat,
            "lon": lon
        }

    except Exception as e:
        # ここでエラー内容を詳しく返すようにします
        import traceback
        error_details = traceback.format_exc()
        print(error_details) # Renderのログに詳細が出ます
        return {"status": "error", "message": str(e), "details": error_details}

