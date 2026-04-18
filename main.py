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
        # --- 経度の変換ロジック (重要) ---
        # HYCOMのlonが0-360の場合、負の値が来たら360足す
        target_lon = lon if lon >= 0 else lon + 360
        
        # もしデータの座標系を確認して 132.5 が 132.5 として存在しない場合
        # データの lon.values を print して確認してみてください。
        
        subset = ds.sel(
            lat=lat,
            lon=target_lon, # 変換後の経度を使用
            method="nearest"
        ).isel(time=0)

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        # .values.item() の前に、値が存在するかチェック
        u_val = subset["water_u"].values
        v_val = subset["water_v"].values

        # 配列が空でないか、または複数入っていないか確認して抽出
        u = float(u_val.flatten()[0])
        v = float(v_val.flatten()[0])

        # 無効値（NaN）のチェック
        if np.isnan(u) or np.isnan(v):
            return {"status": "error", "message": "No data found for this location (陸地の可能性があります)"}

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
