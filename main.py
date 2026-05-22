from fastapi import FastAPI, Query
import xarray as xr
import numpy as np
import requests

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import threading
import os

# =========================
# 🔑 環境変数
# =========================
API_KEY = os.getenv("MSIL_API_KEY")

if API_KEY is None:
    print("❌ WARNING: MSIL_API_KEY is not set")
else:
    print("✅ MSIL_API_KEY loaded")

app = FastAPI()

# =========================
# 🌐 HYCOM設定
# =========================
DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"

ds_local = None
hycom_ready = False

# JST
JST = timezone(timedelta(hours=9))

# =========================
# 🌊 HYCOM読み込み
# =========================
def load_hycom():
    global ds_local, hycom_ready

    print("🌊 HYCOM loading...")

    try:
        ds_local = xr.open_dataset(
            DATA_URL,
            engine="netcdf4",
            decode_times=False
        ).sel(
            lat=slice(30, 46),
            lon=slice(129, 146)
        )

        hycom_ready = True

        print("✅ HYCOM ready")

    except Exception as e:
        print("❌ HYCOM load error:", e)

# =========================
# 🧠 キャッシュ
# =========================
forecast_cache = {}
umishiru_cache = {}

lock = threading.Lock()

CACHE_TTL = 1800  # 30分

# =========================
# 🌊 HYCOM現在流
# =========================
def get_from_hycom(lat, lon):

    if not hycom_ready or ds_local is None:
        return {
            "status": "loading",
            "message": "HYCOM initializing"
        }

    try:
        subset = ds_local.sel(
            lat=lat,
            lon=lon,
            method="nearest"
        ).isel(time=0)

        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values)
        v = float(subset["water_v"].values)

        if np.isnan(u) or np.isnan(v):
            return {
                "status": "error",
                "message": "land"
            }

        speed = np.sqrt(u**2 + v**2) * 1.94384

        direction = (
            np.degrees(np.arctan2(v, u)) + 360
        ) % 360

        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "direction": round(direction, 1),
            "source": "HYCOM"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

# =========================
# 🛳️ 海しる 1時間取得
# =========================
def fetch_umishiru_hour(area_code, hour):

    try:

        # 🎯 JST基準
        now_jst = datetime.now(JST)

        base_jst = now_jst.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        target = base_jst + timedelta(hours=hour)

        # API形式
        time_string = target.strftime("%Y%m%d%H%M")

        url = (
            "https://api.msil.go.jp/tidal-current-prediction/v3/data"
            f"?areaCode={area_code}"
            f"&time={time_string}"
            f"&key={API_KEY}"
        )

        print("🌊 海しるURL:", url)

        r = requests.get(url, timeout=20)

        print("STATUS:", r.status_code)

        if r.status_code != 200:
            print("❌ HTTP ERROR")
            return None

        data = r.json()

        features = data.get("features", [])

        if not features:
            print("❌ features empty")
            print(data)
            return None

        p = features[0]["properties"]

        speed = p.get("currentSpeedKt", 0.0)
        direction = p.get("currentDirection", 0.0)

        return {
            "time": hour,
            "speed": speed,
            "direction": direction
        }

    except Exception as e:
        print("❌ umishiru error:", e)
        return None

# =========================
# 🌊 海しる48時間取得
# =========================
def fetch_48h(area_code):

    print(f"📡 fetch_48h START: {area_code}")

    with ThreadPoolExecutor(max_workers=4) as executor:

        results = list(
            executor.map(
                lambda h: fetch_umishiru_hour(area_code, h),
                range(48)
            )
        )

    filtered = [r for r in results if r]

    print("✅ SUCCESS COUNT:", len(filtered))

    if not filtered:
        return {
            "status": "error",
            "data": []
        }

    return {
        "status": "success",
        "data": filtered
    }

# =========================
# 🌊 海しるバックグラウンド更新
# =========================
def update_umishiru_background(areaCode):

    cache = umishiru_cache.setdefault(
        areaCode,
        {
            "last_good": None,
            "date": None,
            "updating": False,
            "building": False
        }
    )

    try:

        data = fetch_48h(areaCode)

        if data["status"] == "success":

            cache["last_good"] = data
            cache["date"] = datetime.now(JST).date()

            print("✅ cache updated:", areaCode)

    finally:

        with lock:
            cache["updating"] = False

# =========================
# 🌊 海しるメイン取得
# =========================
def get_umishiru(areaCode):

    cache = umishiru_cache.setdefault(
        areaCode,
        {
            "last_good": None,
            "updating": False,
            "date": None,
            "building": False
        }
    )

    # キャッシュある
    if cache["last_good"]:

        with lock:

            if cache.get("updating"):
                return cache["last_good"]

            cache["updating"] = True

        threading.Thread(
            target=update_umishiru_background,
            args=(areaCode,),
            daemon=True
        ).start()

        return cache["last_good"]

    # 初回取得
    cache["building"] = True

    try:

        data = fetch_48h(areaCode)

        if data["status"] == "success":
            cache["last_good"] = data

        return data

    finally:

        cache["building"] = False

# =========================
# 🚀 API
# =========================

@app.get("/")
def root():
    return {
        "status": "ok"
    }

# =========================
# 🌊 現在流
# =========================
@app.get("/current")
def current(
    lat: float = Query(...),
    lon: float = Query(...)
):
    return get_from_hycom(lat, lon)

# =========================
# 🌊 HYCOM予報
# =========================
@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...)):
    """HYCOMから48時間分の潮流予測を取得（最新データ対応・安全版）"""
    if not hycom_ready or ds_local is None:
        return {"status": "loading", "data": []}

    key = f"{round(lat,2)}_{round(lon,2)}"
    now = datetime.utcnow().timestamp()

    if key in forecast_cache:
        cached = forecast_cache[key]
        if now - cached["time"] < CACHE_TTL:
            return cached["data"]

    try:
        # 指定された緯度・経度に一番近い地点を切り出し
        point = ds_local.sel(lat=lat, lon=lon, method="nearest")
        
        # 💡 [重要修正] インデックス（数字）ではなく、時間（time軸）の「実際の値」を使って48時間分を安全に取得します
        # HYCOMのtime軸に格納されている数値の型（基準が何か）を自動判別して時間を生成
        first_time_val = float(ds_local["time"].values[0])
        
        # 日本時間の今日0時を基準にする
        base_jst = datetime.utcnow() + timedelta(hours=9)
        base_utc_start = base_jst.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=9)

        results = []
        for h in range(48):
            target_time = base_utc_start + timedelta(hours=h)
            
            # 2000年基準か1900年基準かを自動で合わせてHYCOM用の時間数値を計算
            # 1つ目のデータが「2000年1月1日からの経過時間」と仮定した場合のズレを逆算
            hycom_start = datetime(2000, 1, 1)
            time_diff_hours = (target_time - hycom_start).total_seconds() / 3600
            
            try:
                # ターゲット時間に一番近いデータを時間指定で直接取得
                subset = point.sel(time=time_diff_hours, method="nearest")
                if "depth" in subset.dims:
                    subset = subset.isel(depth=0)

                u = float(subset["water_u"].values)
                v = float(subset["water_v"].values)

                if np.isnan(u) or np.isnan(v):
                    results.append({"time": h, "speed": 0.0, "direction": 0.0})
                    continue

                results.append({
                    "time": h,
                    "speed": round(np.sqrt(u**2 + v**2) * 1.94384, 2),
                    "direction": round((np.degrees(np.arctan2(v, u)) + 360) % 360, 1)
                })
            except Exception as e:
                # 万が一時間が見つからない時間帯は0で埋めて全体のフリーズを防ぐ
                results.append({"time": h, "speed": 0.0, "direction": 0.0})

        response = {"status": "success", "data": results}

        with lock:
            if len(forecast_cache) > 200:
                while len(forecast_cache) > 150:
                    forecast_cache.pop(next(iter(forecast_cache)), None)

        forecast_cache[key] = {"time": now, "data": response}
        return response
    except Exception as e:
        return {"status": "error", "message": f"HYCOM計算エラー: {str(e)}"}
# =========================
# 🌊 海しる予報
# =========================
@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str):

    print("📍 areaCode:", areaCode)

    if API_KEY is None:
        return {
            "status": "error",
            "message": "MSIL_API_KEY missing"
        }

    return get_umishiru(areaCode)

# =========================
# 🏁 startup
# =========================
@app.on_event("startup")
def startup():

    print("🚀 server startup")

    threading.Thread(
        target=load_hycom,
        daemon=True
    ).start()

# =========================
# ▶️ 起動
# =========================
if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
