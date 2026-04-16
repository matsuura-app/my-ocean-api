from fastapi import FastAPI, Query
import xarray as xr
import numpy as np

app = FastAPI()

# 安定して動く最新のデータURL
DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

@app.get("/current")
def get_current(lat: float = Query(...), lon: float = Query(...)):
    try:
        # サーバーの負担を最小限にする読み込み方 (decode_times=False)
        ds = xr.open_dataset(DATA_URL, decode_times=False)
        
        # 座標を指定（一番近い1点だけを抜き出す）
        subset = ds.sel(lat=lat, lon=lon, depth=0, method="nearest")
        
        # データを取得し、確実に「1つの数字」に変換する魔法の処理
        u = float(np.array(subset.water_u.values).flatten()[0])
        v = float(np.array(subset.water_v.values).flatten()[0])
        
        # 流速計算 (m/s -> knot)
        speed = np.sqrt(u**2 + v**2) * 1.94384
        
        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "lat": lat,
            "lon": lon,
            "message": "開通おめでとうございます！"
        }
    except Exception as e:
        # エラーが出ても止まらないようにする
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

