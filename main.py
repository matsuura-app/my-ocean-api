from fastapi import FastAPI, Query, HTTPException
import xarray as xr
import numpy as np
import requests
import threading
import os
import sys
from datetime import datetime, timedelta, timezone

# =====================================================
# FastAPI / 環境変数
# =====================================================
app = FastAPI()
API_KEY = os.getenv("MSIL_API_KEY")

# =====================================================
# 設定・タイムゾーン (JST / UTC)
# =====================================================
UTC = timezone.utc
JST = timezone(timedelta(hours=9))
RTOFS_TTL = 1800        # 30分

forecast_cache = {}
umishiru_cache = {}
lock = threading.Lock()

# =====================================================
# 🌐 URL確定ロジック (pydap / netcdf4 両対応防御版)
# =====================================================
def get_rtofs_url():
    """
    pytzを完全に排除し、標準datetimeのみでNOAAのURL生存確認を行います。
    """
    now_utc = datetime.now(UTC)

    # サーバーの反映ラグを考慮し、今日・昨日・一昨日の順でデータを探す
    for days_back in [0, 1, 2]:
        target_date = now_utc - timedelta(days=days_back)
        date_str = target_date.strftime("%Y%m%d")

        url_candidates = [
            f"https://nomads.ncep.noaa.gov/dods/rtofs/rtofs_global{date_str}/rtofs_glo_2ds_forecast_3hrly_prog",
            f"https://nomads.ncep.noaa.gov:9090/dods/rtofs/rtofs_global{date_str}/rtofs_glo_2ds_forecast_3hrly_prog",
            f"https://nomads.ncep.noaa.gov/dods/rtofs/rtofs_global{date_str}/rtofs_glo_2ds_forecast_time_prog"
        ]

        for url in url_candidates:
            try:
                # 接続テスト (まずは軽量に開く)
                test = xr.open_dataset(url, engine="netcdf4", decode_times=False, cache=False)
                test.close()
                print(f"🚀 [NOAA CONNECT SUCCESS] URL特定: {url}", flush=True)
                return url
            except Exception as e:
                print(f"⚠️ URLスキップ: {url} | 原因: {e}", flush=True)
                continue

    return None

# =====================================================
# 🔍 内部構造を解析してデータ抽出するメイン関数
# =====================================================
def generate_forecast(lat, lon):
    url = get_rtofs_url()
    if url is None:
        return {"status": "error", "message": "No active NOAA RTOFS dataset found"}

    try:
        # pydapでの接続失敗回避のため、netcdf4を優先しつつフォールバック設計
        try:
            ds = xr.open_dataset(url, engine="netcdf4", decode_times=False, cache=False)
        except Exception:
            ds = xr.open_dataset(url, engine="pydap", decode_times=False, cache=False)

        with ds:
            # 座標軸判定
            lat_name = "lat" if "lat" in ds.coords else "latitude" if "latitude" in ds.coords else None
            lon_name = "lon" if "lon" in ds.coords else "longitude" if "longitude" in ds.coords else None
            
            if not lat_name or not lon_name:
                return {"status": "error", "message": "Required coordinates not found"}

            # 最寄り座標の抽出
            point = ds.sel({lat_name: lat, lon_name: lon}, method="nearest")

            available_vars = list(point.data_vars)
            u_varname = "u" if "u" in available_vars else "u_velocity" if "u_velocity" in available_vars else None
            v_varname = "v" if "v" in available_vars else "v_velocity" if "v_velocity" in available_vars else None

            if not u_varname or not v_varname:
                return {"status": "error", "message": "Velocity variables missing"}

            results = []
            
            # 48時間分 (3時間刻み前提で16ステップ) のループ
            for i in range(16):
                hour_val = i * 3
                try:
                    t_subset = point.isel(time=i)
                    
                    if "ens" in t_subset.dims:
                        t_subset = t_subset.isel(ens=0)
                    if "lev" in t_subset.dims:
                        t_subset = t_subset.isel(lev=0)

                    # NumPyのバージョン依存(3.14環境)を破壊する .item() 化
                    u = float(t_subset[u_varname].values.item())
                    v = float(t_subset[v_varname].values.item())

                    if np.isnan(u) or np.isnan(v) or u > 100000.0 or v > 100000.0:
                        results.append({"time": hour_val, "speed": 0.0, "direction": 0.0})
                        continue

                    speed = round(np.sqrt(u * u + v * v) * 1.94384, 2)
                    direction = round((np.degrees(np.arctan2(v, u)) + 360) % 360, 1)
                    results.append({"time": hour_val, "speed": speed, "direction": direction})

                except Exception as val_err:
                    print(f"❌ [VALUE ERROR] index={i}: {val_err}", flush=True)
                    results.append({"time": hour_val, "speed": 0.0, "direction": 0.0})

            return {"status": "success", "data": results}

    except Exception as e:
        return {"status": "error", "message": f"Fatal Crash: {str(e)}"}

# =====================================================
# 各エンドポイント
# =====================================================
@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...)):
    key = f"{lat:.2f}_{lon:.2f}"
    now = datetime.utcnow().timestamp()

    with lock:
        cache = forecast_cache.get(key)
        if cache:
            if cache.get("data") and cache["data"].get("status") == "success":
                if now - cache["time"] < RTOFS_TTL:
                    return cache["data"]

    data = generate_forecast(lat, lon)
    if data["status"] == "success":
        with lock:
            forecast_cache[key] = {"time": now, "data": data}
    return data

@app.get("/")
def root():
    return {"status": "ok", "mode": "2026-final-pytz-absolute-zero"}
