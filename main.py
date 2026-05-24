import os
from datetime import datetime, timedelta
import numpy as np
import xarray as xr
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# CORS設定（フロントエンドアプリからのアクセスを許可）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"Status": "ok", "mode": "2026-noaa-rtofs-fixed-v2"}

def get_active_rtofs_dataset():
    """
    NOAA RTOFS の OpenDAP サーバーから、現在アクセス可能な有効なデータセットを探します。
    直近3日分の候補URLを巡回し、最初につながったものを返します。
    """
    base_url = "http://nomads.ncep.noaa.gov:9090/dods/rtofs"
    
    # 候補となるデータセット名（アドバイス画像にあったパターンを網羅）
    dataset_patterns = [
        "rtofs_glo_2ds_forecast_3hrly_prog",
        "rtofs_glo_2ds_f006_3hrly_prog"
    ]
    
    # 今日から3日前までの日付をチェック
    now = datetime.utcnow()
    for i in range(3):
        target_date = (now - timedelta(days=i)).strftime("%Y%m%d")
        for pattern in dataset_patterns:
            # URL例: http://nomads.ncep.noaa.gov:9090/dods/rtofs/rtofs20260524/rtofs_glo_2ds_forecast_3hrly_prog
            url = f"{base_url}/rtofs{target_date}/{pattern}"
            print(f"🔍 NOAA接続テスト中: {url}")
            try:
                # テストとして、メタデータ（構造）だけを高速に読み込めるか確認
                ds = xr.open_dataset(url, decode_times=False, cache=False)
                print(f"🚀 [NOAA CONNECT SUCCESS] 有効なURLを発見: {url}")
                return ds, url
            except Exception as e:
                print(f"⚠️ URLスキップ: {url} | 原因: {str(e)}")
                continue
                
    return None, None

def generate_forecast(lat: float, lon: float):
    # 経度を NOAA 基準 (0〜360) に変換
    if lon < 0:
        lon += 360.0

    # 有効なデータセットを自動取得
    ds, active_url = get_active_rtofs_dataset()
    if not ds:
        raise HTTPException(
            status_code=503,
            detail="NOAA RTOFS サーバーが応答しないか、本日のデータがまだ生成されていません。しばらく経ってから再度お試しください。"
        )

    print("=== DATA VARIABLES ===")
    print(ds.data_vars)
    print("======================")

    # 変数名の決定 (u, v または u_velocity, v_velocity)
    u_var = "u" if "u" in ds.data_vars else "u_velocity" if "u_velocity" in ds.data_vars else None
    v_var = "v" if "v" in ds.data_vars else "v_velocity" if "v_velocity" in ds.data_vars else None

    if not u_var or not v_var:
        raise HTTPException(status_code=500, detail="NOAAデータ内の流速変数名(u, v)が見つかりません。")

    try:
        # 最も近い位置のデータを抽出
        point = ds.sel(lat=lat, lon=lon, method="nearest")
        results = []

        # 48時間分（3時間毎なので16ステップ）のデータを取得
        # ループ回数はアドバイス画像に基づき調整
        for h in range(16):
            try:
                subset = point.isel(time=h)
                
                # アドバイスに従い、.values.item() を使って安全に数値化
                u_val = float(subset[u_var].values.item())
                v_val = float(subset[v_var].values.item())

                # 値が欠損（NaN）の場合はスキップ
                if np.isnan(u_val) or np.isnan(v_val):
                    continue

                # 流速 (speed) と 流向 (direction) を計算
                speed = float(np.sqrt(u_val**2 + v_val**2))
                direction = float(np.degrees(np.arctan2(u_val, v_val)))
                if direction < 0:
                    direction += 360.0

                # 3時間毎の予測時間として返却
                results.append({
                    "time": h * 3,
                    "speed": round(speed, 2),
                    "direction": round(direction, 1)
                })
            except Exception as loop_e:
                print(f"❌ [VALUE ERROR] タイムステップ {h} の解析に失敗: {str(loop_e)}")
                # 特定の時間だけデータが壊れている場合は飛ばして次に進む
                continue

        return results

    except Exception as e:
        print(f"❌ [CRITICAL ERROR] データ解析全体に失敗: {str(e)}")
        raise HTTPException(status_code=500, detail=f"データ解析エラー: {str(e)}")
    finally:
        ds.close()

@app.get("/forecast")
def forecast(
    lat: float = Query(..., description="Latitude (e.g. 33.3)"),
    lon: float = Query(..., description="Longitude (e.g. 133.5)")
):
    print(f"📡 NOAA RTOFS FETCH: lat={lat}, lon={lon}")
    data = generate_forecast(lat, lon)
    return {
        "status": "success",
        "lat": lat,
        "lon": lon,
        "data": data
    }
