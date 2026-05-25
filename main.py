import os
import threading
import sqlite3
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
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

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

JST = timezone(timedelta(hours=9))
umishiru_cache = {}
lock = threading.Lock()

# =========================================================
# 💾 データベース管理 (気象庁潮汐用)
# =========================================================
def get_conn():
    conn = sqlite3.connect("tides.db", timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tides (
        point TEXT,
        datetime TEXT,
        height REAL,
        PRIMARY KEY(point, datetime)
    )
    """)
    conn.commit()
    conn.close()

# =========================================================
# 🌊 気象庁 潮位取得・3年分管理ロジック
# =========================================================
def fetch_and_save_jma_year(point_code: str, year: int):
    station_map = {"kure": "Q9", "tokyo": "TK", "osaka": "OS"}
    jma_code = station_map.get(point_code.lower(), point_code.upper())
    url = f"https://www.data.jma.go.jp/kaiyou/data/db/tide/suisan/txt/{year}/{jma_code}.txt"
    print(f"📡 Fetching JMA tide data for {year}: {url}", flush=True)

    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200: return False

        lines = r.text.splitlines()
        conn = get_conn()
        cur = conn.cursor()

        saved_count = 0
        for line in lines:
            if len(line) < 72: continue
            try:
                line_year = 2000 + int(line[72:74])
                line_month = int(line[74:76])
                line_day = int(line[76:78])
            except ValueError: continue

            hourly_part = line[0:72]
            for hour in range(24):
                start_idx = hour * 3
                height_str = hourly_part[start_idx:start_idx+3].strip()
                if not height_str: continue
                try: height = float(height_str)
                except ValueError: continue

                dt_str = f"{line_year}-{line_month:02d}-{line_day:02d} {hour:02d}:00:00"
                cur.execute("""
                    INSERT OR REPLACE INTO tides (point, datetime, height)
                    VALUES (?, ?, ?)
                """, (jma_code, dt_str, height))
                saved_count += 1

        conn.commit()
        conn.close()
        print(f"SUCCESS: Saved {saved_count} items for {jma_code} ({year})", flush=True)
        return True
    except Exception as e:
        print("JMA Data Parse Error:", e, flush=True)
        return False

def cleanup_old_tides(current_year: int):
    oldest_valid_year = current_year - 1
    threshold_date = f"{oldest_valid_year}-01-01 00:00:00"
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM tides WHERE datetime < ?", (threshold_date,))
        deleted_rows = cur.rowcount
        conn.commit()
        conn.close()
        if deleted_rows > 0:
            print(f"CLEANUP: {deleted_rows}件の古い潮汐データを削除しました", flush=True)
    except Exception as e:
        print("Cleanup Error:", e, flush=True)

# =========================================================
# 🛳️ 海しる潮流データ取得ロジック (元の完全作動版を完全再現)
# =========================================================
def fetch_umishiru_hour(area_code, hour_offset):
    try:
        base_jst = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
        target = base_jst + timedelta(hours=hour_offset)
        time_string = target.strftime("%Y%m%d%H%M")

        url = "https://api.msil.go.jp/tidal-current-prediction/v3/data"
        params = {"areaCode": area_code, "time": time_string, "key": API_KEY}

        r = session.get(url, params=params, timeout=10)
        if r.status_code != 200: return None

        data = r.json()
        features = data.get("features", [])
        if not features: return None

        p = features[0].get("properties", {})
        raw_speed = p.get("currentSpeedKt", 0.0)
        raw_direction = p.get("currentDirection", 0.0)

        # Swift安全防御キャスト
        speed = float(raw_speed) if raw_speed is not None else 0.0
        direction = float(raw_direction) if raw_direction is not None else 0.0

        # 🌟あなたのベースコードの秘密：Swiftはここに「連番(整数)」を求めています！
        return {
            "time": hour_offset,
            "speed": speed,
            "direction": direction
        }
    except:
        return None

def fetch_48h_parallel(area_code):
    print(f"📡 【海しるAPI】{area_code} の48時間並列取得を開始します", flush=True)
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(
            lambda h: fetch_umishiru_hour(area_code, h),
            range(48)
        ))
    filtered = [r for r in results if r]
    filtered.sort(key=lambda x: x["time"])
    if not filtered:
        return {"status": "error", "data": []}
    return {"status": "success", "data": filtered}

# =========================================================
# 天気予報データ取得ロジック
# =========================================================
def fetch_weather_logic(lat, lon):
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,windspeed_10m,winddirection_10m,weathercode",
        "forecast_days": 7, "timezone": "Asia/Tokyo"
    }
    try:
        r = session.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=10)
        if r.status_code == 200: return r.json()
    except: pass
    return None

# =========================================================
# 🚀 APIエンドポイント一覧
# =========================================================
@app.get("/")
def root():
    return {"status": "ok", "server": "marine-v15-perfect-final"}

@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str = Query(..., alias="areaCode")):
    """海しる潮流予測（爆速並列・6時間キャッシュ・Swift時間完全復元版）"""
    if not API_KEY or API_KEY.strip() == "":
        return {"status": "error", "message": "MSIL_API_KEY is missing"}

    now_jst = datetime.now(JST)
    with lock:
        cache = umishiru_cache.get(areaCode)
        if cache and cache.get("expires") > now_jst:
            print(f"⚡ 【キャッシュHIT】areaCode={areaCode}", flush=True)
            return cache["data"]

    data = fetch_48h_parallel(areaCode)
    if data and data["status"] == "success":
        with lock:
            umishiru_cache[areaCode] = {
                "expires": now_jst + timedelta(hours=6),
                "data": data
            }
        return data
    return {"status": "error", "message": "Failed to fetch from Umishiru"}

@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...)):
    """本物の天気予報データを取得"""
    w_data = fetch_weather_logic(lat, lon)
    if w_data:
        return {"status": "success", "lat": lat, "lon": lon, "weather": w_data}
    return {"status": "error", "message": "Failed to fetch weather"}

@app.get("/tide")
def get_tide(point: str):
    """JMA潮汐自動管理API (カレンダー対応2年分返却版)"""
    now = datetime.now(JST)
    current_year = now.year
    station_map = {"kure": "Q9", "tokyo": "TK", "osaka": "OS"}
    jma_code = station_map.get(point.lower(), point.upper())

    cleanup_old_tides(current_year)

    start_date_str = (now - timedelta(days=180)).strftime("%Y-%m-%d 00:00:00")
    end_date_str = (now + timedelta(days=545)).strftime("%Y-%m-%d 23:59:59")

    def query_db():
        conn = get_conn()
        cur = conn.cursor()
        res = cur.execute("""
            SELECT datetime, height FROM tides
            WHERE point = ? AND datetime BETWEEN ? AND ? ORDER BY datetime ASC
        """, (jma_code, start_date_str, end_date_str)).fetchall()
        conn.close()
        return res

    rows = query_db()
    if len(rows) < 15000:
        fetch_and_save_jma_year(jma_code, current_year - 1)
        fetch_and_save_jma_year(jma_code, current_year)
        fetch_and_save_jma_year(jma_code, current_year + 1)
        rows = query_db()

    return {
        "status": "success", "point": jma_code,
        "data": [{"time": r["datetime"], "height": r["height"]} for r in rows]
    }

@app.on_event("startup")
def startup():
    init_db()

@app.get("/routes")
def routes():
    return [route.path for route in app.routes]
