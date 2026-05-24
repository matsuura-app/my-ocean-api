from fastapi import FastAPI, Query  # 💡 from を小文字に修正
import xarray as xr
import numpy as np
import requests
import threading
import os
from datetime import datetime, timedelta, timezone

# =====================================================
# FastAPI / 環境変数
# =====================================================
app = FastAPI()
API_KEY = os.getenv("MSIL_API_KEY")

# =====================================================
# 設定・タイムゾーン
# =====================================================
JST = timezone(timedelta(hours=9))
RTOFS_TTL = 1800        # 30分

forecast_cache = {}
umishiru_cache = {}
lock = threading.Lock()

# =====================================================
# 🌐 NOAA RTOFS の「今日の日付」URLを動的に生成する関数
# =====================================================
def get_rtofs_url():
    """
    NOAAのサーバー仕様に合わせ、今日の日付のデータセットURLを自動生成します。
    """
    now_utc = datetime.utcnow()
    # サーバーの更新ラグを考慮し、直近2日間の有効なURLを探索
    for days_back in [0, 1, 2]:
        target_date = now_utc - timedelta(days=days_back)
        date_str = target_date.strftime("%Y%m%d")
        url = f"http://nomads.ncep.noaa.gov:9090/dods/rtofs/rtofs_global{date_str}/rtofs_ge_2d_forecast"
        
        # 接続テスト (HEADリクエストでデータセットが存在するか1秒だけ確認)
        try:
            r = requests.head(url, timeout=1.5)
            if r.status_code == 200:
                return url
        except Exception:
            continue
            
    # 万が一のフォールバック用（固定リンク）
    return "http://nomads.ncep.noaa.gov:9090/dods/rtofs/rtofs_global/rtofs_ge_2d_forecast"

# =====================================================
# 陸地を回避して最も近い「海」の座標を探すロジック (RTOFS仕様)
# =====================================================
def find_sea_point(ds, lat, lon):
    offsets = [0, 0.04, -0.04, 0.08, -0.08]
    for lat_off in offsets:
        for lon_off in offsets:
            try:
                # 💡 RTOFSは軸の名前が「latitude」「longitude」のため、マッピングを修正
                point = ds.sel(latitude=lat + lat_off, longitude=lon + lon_off, method="nearest")
                subset = point.isel(time=0)
                
                # RTOFSの潮流変数名: u_velocity
                u = float(subset["u_velocity"].values)
                
                # RTOFSの陸地判定: 陸地は「9.999e+20」という超巨大な数字で埋め尽くされている
                if np.isnan(u) or u > 100000.0:
                    continue
                return point
            except Exception:
                continue
    return None

# =====================================================
# NOAA RTOFS FORECAST 予測生成 (48時間分)
# =====================================================
def generate_forecast(lat, lon):
    url = get_rtofs_url()
    print(f"📡 [NOAA RTOFS FETCH] ターゲットURL: {url}")

    try:
        # 💡 decode_times=False を追加して高速化＆メモリ不足によるRenderのクラッシュを防止
        with xr.open_dataset(url, engine="netcdf4", decode_times=False, cache=False) as ds:
            point = find_sea_point(ds, lat, lon)
            if point is None:
                return {"status": "error", "message": "sea point not found"}

            results = []
            
            # RTOFSの time 軸（0番目＝最新予測）から1時間ずつ48時間分を抽出
            for h in range(48):
                try:
                    subset = point.isel(time=h)

                    # RTOFSの東西(u)・南北(v)の潮流流速
                    u = float(subset["u_velocity"].values)
                    v = float(subset["v_velocity"].values)

                    # 陸地ダミー値（9.999e+20）または NaN のスキップ処理
                    if np.isnan(u) or np.isnan(v) or u > 100000.0 or v > 100000.0:
                        results.append({"time": h, "speed": 0.0, "direction": 0.0})
                        continue

                    # 流速(knot)と流向(degree)の計算
                    speed = round(np.sqrt(u * u + v * v) * 1.94384, 2)
                    direction = round((np.degrees(np.arctan2(v, u)) + 360) % 360, 1)

                    results.append({"time": h, "speed": speed, "direction": direction})
                except Exception:
                    results.append({"time": h, "speed": 0.0, "direction": 0.0})

            return {"status": "success", "data": results}

    except Exception as e:
        print("❌ NOAA RTOFS fatal error:", e)
        return {"status": "error", "message": str(e)}

# =====================================================
# RTOFS ENDPOINT
# =====================================================
@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...)):
    key = f"{lat:.2f}_{lon:.2f}"
    now = datetime.utcnow().timestamp()

    with lock:
        cache = forecast_cache.get(key)
        if cache:
            # ゾンビキャッシュ防止ガード
            if cache.get("data") and cache["data"].get("status") == "success" and cache["data"].get("data"):
                if now - cache["time"] < RTOFS_TTL:
                    print(f"⚡ NOAA RTOFS CACHE HIT {key}")
                    return cache["data"]

    print(f"📡 NOAA RTOFS FETCH {key}")
    data = generate_forecast(lat, lon)

    if data["status"] == "success" and data.get("data"):
        with lock:
            forecast_cache[key] = {"time": now, "data": data}

    return data

# =====================================================
# UMISHIRU FETCH
# =====================================================
def fetch_umishiru_hour(area_code, hour, base_jst):
    try:
        target = base_jst + timedelta(hours=hour)
        time_string = target.strftime("%Y%m%d%H%M")

        url = (
            "https://api.msil.go.jp/tidal-current-prediction/v3/data"
            f"?areaCode={area_code}&time={time_string}&key={API_KEY}"
        )
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None

        data = r.json()
        features = data.get("features", [])
        if not features:
            return {"time": hour, "speed": 0.0, "direction": 0.0}

        p = features[0]["properties"]
        return {
            "time": hour,
            "speed": p.get("currentSpeedKt", 0.0),
            "direction": p.get("currentDirection", 0.0)
        }
    except Exception:
        return None

def fetch_48h(area_code, base_jst):
    results = []
    for h in range(48):
        r = fetch_umishiru_hour(area_code, h, base_jst)
        if r:
            results.append(r)

    if not results:
        return {"status": "error", "data": []}
    return {"status": "success", "data": results}

# =====================================================
# UMISHIRU ENDPOINT (キャッシュ管理をJST日付で完全固定)
# =====================================================
@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str):
    if API_KEY is None:
        return {"status": "error", "message": "MSIL_API_KEY missing"}

    now_jst = datetime.now(JST)
    today_date = now_jst.date()

    with lock:
        cache = umishiru_cache.get(areaCode)
        if cache and cache.get("date") == today_date:
            if cache.get("data") and cache["data"].get("status") == "success":
                print(f"⚡ UMISHIRU HIT {areaCode}")
                return cache["data"]

    print(f"📡 UMISHIRU FETCH {areaCode}")
    base_jst = now_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    data = fetch_48h(areaCode, base_jst)

    if data["status"] == "success":
        with lock:
            umishiru_cache[areaCode] = {
                "date": today_date,
                "data": data
            }

    return data

# =====================================================
# ROOT
# =====================================================
@app.get("/")
def root():
    return {"status": "ok", "mode": "2026-noaa-rtofs-fixed"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
