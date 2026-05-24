import os
import threading
from datetime import datetime, timedelta, timezone
import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# CONFIG & ENVIRONMENT
# =========================================================
API_KEY = os.getenv("MSIL_API_KEY")

session = requests.Session()
session.headers.update({
    "Origin": "https://my-ocean-api.onrender.com",
    "Referer": "https://my-ocean-api.onrender.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

# =========================================================
# FASTAPI & CORS
# =========================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# TIMEZONE & RENDER MEMORY CACHE
# =========================================================
JST = timezone(timedelta(hours=9))
umishiru_cache = {}
lock = threading.Lock()

# =========================================================
# UMISHIRU FETCH LOGIC
# =========================================================
def fetch_umishiru_48h_bulk(area_code, base_jst):
    time_string = base_jst.strftime("%Y%m%d%H%M")
    url = "https://api.msil.go.jp/tidal-current-prediction/v3/data"
    params = {"areaCode": area_code, "time": time_string, "key": API_KEY}
    
    print(f"📡 【海しるAPI送信】areaCode: {area_code} | time: {time_string}", flush=True)

    try:
        r = session.get(url, params=params, timeout=(5, 15))
        if r.status_code != 200:
            print(f"⚠️ 海しるAPIエラー: STATUS {r.status_code}", flush=True)
            return None

        data = r.json()
        features = data.get("features", [])
        if not features:
            return None

        results = []
        for feature in features:
            p = feature.get("properties", {})
            raw_time = p.get("time")
            raw_speed = p.get("currentSpeedKt")
            raw_direction = p.get("currentDirection")
            
            try:
                speed = float(raw_speed) if raw_speed is not None else 0.0
            except:
                speed = 0.0
                
            try:
                direction = float(raw_direction) if raw_direction is not None else 0.0
            except:
                direction = 0.0

            results.append({
                "time": raw_time,
                "speed": speed,
                "direction": direction
            })
        return {"status": "success", "data": results}
    except Exception as e:
        print(f"❌ 海しる通信例外: {e}", flush=True)
        return None

# =========================================================
# APP ENDPOINTS
# =========================================================
@app.get("/")
def root():
    return {"status": "ok", "server": "marine-v9-debug-active"}

@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str = Query(..., alias="areaCode")):
    if not API_KEY or API_KEY.strip() == "":
        return {"status": "error", "message": "MSIL_API_KEY is missing."}

    now_jst = datetime.now(JST)
    with lock:
        cache = umishiru_cache.get(areaCode)
        if cache and cache.get("expires") > now_jst:
            return cache["data"]

    base_jst = now_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    data = fetch_umishiru_48h_bulk(areaCode, base_jst)

    if data and data["status"] == "success":
        with lock:
            umishiru_cache[areaCode] = {
                "expires": now_jst + timedelta(hours=6),
                "data": data
            }
        return data
    return {"status": "error", "message": "Failed to fetch from Umishiru."}

@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...)):
    # 簡易天気用プレースホルダー
    return {"status": "success", "lat": lat, "lon": lon, "weather": None}

@app.get("/tide")
def tide(port_code: str, year: int, month: int, day: int):
    return {"status": "success"}

# =========================================================
# 🔍 相手のAI推奨：ルート確認＆ファイル読み込みチェック用
# =========================================================
@app.get("/routes")
def routes():
    """現在FastAPIに登録されているURLルートの一覧を返します"""
    return [route.path for route in app.routes]

# 起動時にログに強制出力させてファイル生存確認を行う
print("🔥 CURRENT FILE LOADED: marine-final-v9-origin-fixed", flush=True)
