from fastapi import FastAPI, Query
import xarray as xr
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import threading
import sqlite3
import os
import json

# =========================
# 🔑 環境変数
# =========================
API_KEY = os.getenv("MSIL_API_KEY")
if API_KEY is None:
    print("WARNING: MSIL_API_KEY is not set")

app = FastAPI()

# =========================
# 💾 データベース管理
# =========================
def get_conn():
    conn = sqlite3.connect(
        "tides.db",
        timeout=10,
        check_same_thread=False
    )
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

# =========================
# 🌊 気象庁 潮位取得・3年分管理ロジック
# =========================
def fetch_and_save_jma_year(point_code: str, year: int):
    """
    気象庁から指定された年の潮汐テキストデータを取得してDBに保存する
    """
    station_map = {
        "kure": "Q9",
        "tokyo": "TK",
        "osaka": "OS",
    }
    jma_code = station_map.get(point_code.lower(), point_code.upper())
    url = f"https://www.data.jma.go.jp/kaiyou/data/db/tide/suisan/txt/{year}/{jma_code}.txt"
    print(f"Fetching JMA tide data for {year}: {url}")

    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            print(f"JMA Error ({r.status_code}): データがまだ公開されていないか、コードが違います。")
            return False

        lines = r.text.splitlines()
        conn = get_conn()
        cur = conn.cursor()

        saved_count = 0
        for line in lines:
            if len(line) < 72:
                continue
            try:
                line_year = 2000 + int(line[72:74])
                line_month = int(line[74:76])
                line_day = int(line[76:78])
            except ValueError:
                continue

            hourly_part = line[0:72]

            for hour in range(24):
                start_idx = hour * 3
                height_str = hourly_part[start_idx:start_idx+3].strip()
                if not height_str:
                    continue
                try:
                    height = float(height_str)
                except ValueError:
                    continue

                dt_str = f"{line_year}-{line_month:02d}-{line_day:02d} {hour:02d}:00:00"
                
                # 🎯【修正完了】point_codeの代わりに、統一されたjma_codeでDBに保存します
                cur.execute("""
                    INSERT OR REPLACE INTO tides (point, datetime, height)
                    VALUES (?, ?, ?)
                """, (jma_code, dt_str, height))
                saved_count += 1

        conn.commit()
        conn.close()
        print(f"SUCCESS: Saved {saved_count} items for {jma_code} ({year})")
        return True
    except Exception as e:
        print("JMA Data Parse Error:", e)
        return False

def cleanup_old_tides(current_year: int):
    """
    過去1年（昨年）より古いデータをDBから自動削除するクリーンアップ処理
    """
    oldest_valid_year = current_year - 1
    threshold_date = f"{oldest_valid_year}-01-01 00:00:00"
    
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM tides
            WHERE datetime < ?
        """, (threshold_date,))
        deleted_rows = cur.rowcount
        conn.commit()
        conn.close()
        if deleted_rows > 0:
            print(f"CLEANUP: {deleted_rows}件の古い潮汐データを削除しました（{oldest_valid_year}年より前）")
    except Exception as e:
        print("Cleanup Error:", e)

# =========================
# 🌐 HYCOM設定
# =========================
DATA_URL = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z"
ds_local = None
hycom_ready = False

def load_hycom():
    global ds_local, hycom_ready
    print("HYCOM loading...")
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
        print("HYCOM ready")
    except Exception as e:
        print("HYCOM load error:", e)

# =========================
# 🧠 キャッシュ＆ロック
# =========================
forecast_cache = {}
umishiru_cache = {}
lock = threading.Lock()
CACHE_TTL = 1800  # 30分

# =========================
# 🌊 HYCOM現在流計算ロジック
# =========================
def get_from_hycom(lat, lon):
    if not hycom_ready or ds_local is None:
        return {"status": "loading", "message": "HYCOM initializing"}

    try:
        subset = ds_local.sel(lat=lat, lon=lon, method="nearest").isel(time=0)
        if "depth" in subset.dims:
            subset = subset.isel(depth=0)

        u = float(subset["water_u"].values)
        v = float(subset["water_v"].values)

        if np.isnan(u) or np.isnan(v):
            return {"status": "error", "message": "land"}

        speed = np.sqrt(u**2 + v**2) * 1.94384
        direction = (np.degrees(np.arctan2(v, u)) + 360) % 360

        return {
            "status": "success",
            "velocity_knot": round(speed, 2),
            "direction": round(direction, 1),
            "source": "HYCOM"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =========================
# 🛳️ 海しる潮流データ取得ロジック
# =========================
def fetch_umishiru_hour(area_code, hour):
    try:
        base = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        target = base + timedelta(hours=hour)
        time_string = target.strftime("%Y%m%d%H%M")

        url = (
            "https://api.msil.go.jp/tidal-current-prediction/v3/data"
            f"?areaCode={area_code}"
            f"&time={time_string}"
            f"&key={API_KEY}"
        )
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None

        data = r.json()
        features = data.get("features", [])
        if not features:
            return None

        p = features[0]["properties"]
        return {
            "time": hour,
            "speed": p.get("currentSpeedKt", 0.0),
            "direction": p.get("currentDirection", 0.0)
        }
    except Exception as e:
        print("umishiru error:", e)
        return None

def fetch_48h(area_code):
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(
            lambda h: fetch_umishiru_hour(area_code, h),
            range(48)
        ))
    filtered = [r for r in results if r]
    if not filtered:
        return {"status": "error", "data": []}
    return {"status": "success", "data": filtered}

def update_umishiru_background(areaCode):
    cache = umishiru_cache.setdefault(areaCode, {
        "last_good": None, "date": None, "updating": False, "building": False
    })
    try:
        data = fetch_48h(areaCode)
        if data["status"] == "success":
            cache["last_good"] = data
            cache["date"] = datetime.utcnow().date()
    finally:
        with lock:
            cache["updating"] = False

def get_umishiru(areaCode):
    cache = umishiru_cache.setdefault(areaCode, {
        "last_good": None, "updating": False, "date": None, "building": False
    })
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

    cache["building"] = True
    try:
        data = fetch_48h(areaCode)
        if data["status"] == "success":
            cache["last_good"] = data
        return data
    finally:
        cache["building"] = False

# =========================
# 🚀 APIエンドポイント一覧
# =========================

@app.get("/current")
def current(lat: float = Query(...), lon: float = Query(...)):
    """HYCOMから現在の流向・流速を取得"""
    return get_from_hycom(lat, lon)

@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...)):
    """HYCOMから48時間分の潮流予測を取得"""
    if not hycom_ready or ds_local is None:
        return {"status": "loading", "data": []}

    key = f"{round(lat,2)}_{round(lon,2)}"
    now = datetime.utcnow().timestamp()

    if key in forecast_cache:
        cached = forecast_cache[key]
        if now - cached["time"] < CACHE_TTL:
            return cached["data"]

    try:
        point = ds_local.sel(lat=lat, lon=lon, method="nearest")
        results = []
        utc_now = datetime.utcnow()
        base_utc = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)

        hycom_start = datetime(2000, 1, 1)
        first_time = float(ds_local["time"].values[0])
        first_datetime = hycom_start + timedelta(hours=first_time)
        offset_hours = int((base_utc - first_datetime).total_seconds() / 3600)

        for h in range(48):
            idx = offset_hours + h
            subset = point.isel(time=idx)
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

        response = {"status": "success", "data": results}

        with lock:
            if len(forecast_cache) > 200:
                while len(forecast_cache) > 150:
                    forecast_cache.pop(next(iter(forecast_cache)), None)

        forecast_cache[key] = {"time": now, "data": response}
        return response
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/umishiru_forecast")
def umishiru_forecast(areaCode: str):
    """海しるAPIから潮流予測を取得"""
    return get_umishiru(areaCode)

@app.get("/tide")
def get_tide(point: str):
    """
    カレンダー特化型API（過去半年 + 当年1年分 + 来年半年 = 計2年分を返却）
    データが足りない場合はバックグラウンドで気象庁と同期し、タイムアウトを防ぎます。
    """
    now = datetime.utcnow()
    current_year = now.year

    station_map = {"kure": "Q9", "tokyo": "TK", "osaka": "OS"}
    jma_code = station_map.get(point.lower(), point.upper())

    # 古いデータの自動削除
    cleanup_old_tides(current_year)

    start_date_dt = now - timedelta(days=180)
    end_date_dt = now + timedelta(days=180 + 365)

    start_date_str = start_date_dt.strftime("%Y-%m-%d 00:00:00")
    end_date_str = end_date_dt.strftime("%Y-%m-%d 23:59:59")

    # DBからデータを取得
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT datetime, height
        FROM tides
        WHERE point = ? AND datetime BETWEEN ? AND ?
        ORDER BY datetime ASC
    """, (jma_code, start_date_str, end_date_str)).fetchall()
    conn.close()

    # 🎯 データの準備ができていない（空っぽ、または件数が大幅に足りない）場合
    if len(rows) < 1000:
        print(f"⚠️ {jma_code} のデータがDBにありません。バックグラウンドで気象庁から同期を開始します...")
        
        # 💡 重い通信を別スレッド（バックグラウンド）で回し、FastAPI自体は即座に応答を返す
        def sync_task():
            fetch_and_save_jma_year(jma_code, current_year - 1)
            fetch_and_save_jma_year(jma_code, current_year)
            fetch_and_save_jma_year(jma_code, current_year + 1)
            print(f"✅ {jma_code} の3年分データの同期が完了しました！")

        threading.Thread(target=sync_task, daemon=True).start()

        # Swift側が「null」と判定して、しばらくして自動再試行できるようにメッセージを返却
        return "null"

    # 通常通りデータが揃っている場合は、綺麗に整形して返却
    return {
        "status": "success",
        "point": jma_code,
        "total_records": len(rows),
        "data": [
            {"time": r["datetime"], "height": r["height"]}
            for r in rows
        ]
    }

# =========================
# 🏁 起動処理
# =========================
@app.on_event("startup")
def startup():
    init_db()
    os.makedirs("data", exist_ok=True)

    # HYCOMバックグラウンド読み込み
    threading.Thread(target=load_hycom, daemon=True).start()

    # 海しる warmup
    try:
        threading.Thread(
            target=update_umishiru_background,
            args=("default",),
            daemon=True
        ).start()
    except Exception as e:
        print("warmup failed:", e)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
