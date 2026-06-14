import os
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import time

import requests
import xarray as xr
import numpy as np

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# CONFIG
# =========================================================

API_KEY = os.getenv("MSIL_API_KEY")

JST = timezone(timedelta(hours=9))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

session = requests.Session()

session.headers.update({
    "Origin": "https://my-ocean-api.onrender.com",
    "Referer": "https://my-ocean-api.onrender.com/",
    "User-Agent": "Mozilla/5.0"
})

# =========================================================
# LOCK & CACHE
# =========================================================

lock = threading.Lock()

# キャッシュ
umishiru_cache = {}
forecast_cache = {}
# =========================================================
# HYCOM CONFIG
# =========================================================

DATA_URL = (
    "https://tds.hycom.org/thredds/dodsC/"
    "FMRC_ESPC-D-V02_uv3z/"
    "FMRC_ESPC-D-V02_uv3z_best.ncd"
)

ds_local = None
hycom_ready = False

# =========================================================
# HYCOM LOAD
# =========================================================
def load_hycom():
    global ds_local, hycom_ready

    hycom_ready = False   # ★これ必須

    print("HYCOM loading...", flush=True)

    try:
        ds = xr.open_dataset(
            DATA_URL,
            engine="netcdf4",
            decode_times=False,
        ).sel(
            lat=slice(30, 46),
            lon=slice(129, 146)
        )

        with lock:
            ds_local = ds
            hycom_ready = True

        print("HYCOM loaded", flush=True)

    except Exception as e:
        hycom_ready = False
        print(f"HYCOM load error: {e}", flush=True)
# =========================================================
# HYCOM WATCHDOG
# =========================================================
def hycom_watchdog():
    global ds_local
    global hycom_ready
    while True:
        test_ds = None
        old_ds = None
        try:
            test_ds = xr.open_dataset(
                DATA_URL,
                engine="netcdf4",
                decode_times=False
            )

            new_time_size = test_ds.sizes.get(
                "time",
                0
            )
            # 初回復旧
            if ds_local is None:
                print(
                    "HYCOM first load from watchdog",
                    flush=True
                )
                new_ds = xr.open_dataset(
                    DATA_URL,
                    engine="netcdf4",
                    decode_times=False,
                ).sel(
                    lat=slice(30, 46),
                    lon=slice(129, 146)
                )
                with lock:
                    ds_local = new_ds
                    hycom_ready = True
            # 更新検知
            else:
                old_time_size = ds_local.sizes.get(
                    "time",
                    0
                )
                if new_time_size != old_time_size:
                    print(
                        "🔄 HYCOM updated",
                        flush=True
                    )
                    new_ds = xr.open_dataset(
                        DATA_URL,
                        engine="netcdf4",
                        decode_times=False,
                        chunks={}
                    ).sel(
                        lat=slice(30, 46),
                        lon=slice(129, 146)
                    )
                    with lock:
                        old_ds = ds_local
                        ds_local = new_ds
                        hycom_ready = True
                    time.sleep(5)
                    if old_ds is not None:
                        try:
                            old_ds.close()
                        except Exception:
                            pass

        except Exception as e:
            print(
                f"HYCOM not reachable: {e}",
                flush=True
            )

        finally:

            if test_ds is not None:

                try:
                    test_ds.close()
                except Exception:
                    pass

        # 12時間ごと
        time.sleep(12 * 3600)

# =========================================================
# HYCOM CURRENT
# =========================================================

def get_from_hycom(lat, lon):

    if ds_local is None:

        return {
            "status": "loading",
            "message": "HYCOM initializing"
        }

    try:

        subset = ds_local.sel(
            lat=lat,
            lon=lon,
            method="nearest"
        )

        subset = subset.isel(time=0)

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

# =========================================================
# HYCOM FORECAST BUILDER (指摘1: estimated_time のバグを修正)
# =========================================================
def build_forecast_response(ds, lat, lon):
    try:
        subset = ds.sel(lat=lat, lon=lon, method="nearest")
        # time_values からの直接計算をやめ、h を使う形に修正
        results = []
        
        max_time = min(48, subset.sizes["time"])
        base_time = datetime.utcnow()
        
        for h in range(max_time):
            try:
                data = subset.isel(time=h)

                if "depth" in data.dims:
                    data = data.isel(depth=0)

                u = float(data["water_u"].values)
                v = float(data["water_v"].values)

                if np.isnan(u) or np.isnan(v):
                    continue

                speed = np.sqrt(u**2 + v**2) * 1.94384
                direction = (np.degrees(np.arctan2(v, u)) + 360) % 360

                # ★修正: HYCOMのシリアル値ではなく、インデックス h (0〜47) をそのまま時間に使う
                results.append({
                    "hour_offset": float(h),
                    "estimated_time": (base_time + timedelta(hours=h)).isoformat(),
                    "speed": round(speed, 2),
                    "direction": round(direction, 1)
                })
            except Exception:
                continue

        return {"status": "success", "data": results}
    except Exception as e:
        return {"status": "error", "message": str(e), "data": []}

# =========================================================
# UMISHIRU
# =========================================================

def fetch_umishiru_hour(
    area_code,
    hour_offset
):

    try:

        base_jst = datetime.now(JST).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        target = base_jst + timedelta(
            hours=hour_offset
        )

        time_string = target.strftime(
            "%Y%m%d%H%M"
        )

        url = (
            "https://api.msil.go.jp/"
            "tidal-current-prediction/v3/data"
        )

        params = {
            "areaCode": area_code,
            "time": time_string,
            "key": API_KEY
        }

        r = session.get(
            url,
            params=params,
            timeout=20
        )

        if r.status_code != 200:
            return None

        data = r.json()

        features = data.get(
            "features",
            []
        )

        if not features:
            return None

        p = features[0].get(
            "properties",
            {}
        )

        speed = float(
            p.get("currentSpeedKt", 0.0) or 0.0
        )

        direction = float(
            p.get("currentDirection", 0.0) or 0.0
        )

        return {
            "time": hour_offset,
            "speed": speed,
            "direction": direction
        }

    except Exception as e:

        print(
            f"umishiru fetch error: {e}",
            flush=True
        )

        return None

def fetch_48h_parallel(area_code):

    with ThreadPoolExecutor(
        max_workers=8
    ) as executor:

        results = list(
            executor.map(
                lambda h: fetch_umishiru_hour(
                    area_code,
                    h
                ),
                range(48)
            )
        )

    filtered = [
        r for r in results if r
    ]

    filtered.sort(
        key=lambda x: x["time"]
    )

    if not filtered:

        return {
            "status": "error",
            "data": []
        }

    return {
        "status": "success",
        "generated_at": datetime.now(JST).isoformat(),
        "data": filtered
    }
# =========================================================
# UMISHIRU API
# =========================================================

@app.get("/umishiru_forecast")
def umishiru_forecast(
    areaCode: str = Query(
        ...,
        alias="areaCode"
    )
):
    if not API_KEY:

        return {
            "status": "error",
            "message": "MSIL_API_KEY missing"
        }

    with lock:
        cache = umishiru_cache.get(areaCode)

    # =====================================================
    # キャッシュ存在時
    # =====================================================

    if cache:
        
        cached_data = cache["data"]

        return cached_data

    # =====================================================
    # 初回取得
    # =====================================================
    data = fetch_48h_parallel(areaCode)

    if data["status"] == "success":

        with lock:

            umishiru_cache[areaCode] = {
                "data": data
            }

    return data

# =========================================================
# API
# =========================================================

@app.get("/")
def root():

    return {
        "status": "ok",
        "server": "marine-final",
        "hycom_ready": hycom_ready
    }


# =========================================================
# API: CURRENT (修正: 陸地バグ対応、メモリキャッシュから即返却)
# =========================================================
@app.get("/current")
def current(
    lat: float = Query(...),
    lon: float = Query(...)
):
    key = f"{round(lat,2)}_{round(lon,2)}"
    with lock:
        cache = forecast_cache.get(key)
        
    # ★指摘2の修正: cacheがあり、かつdataが空っぽ（陸地）ではない場合のみ0番目を取り出す
    if cache and len(cache.get("data", [])) > 0:
        first_hour = cache["data"][0]
        return {
            "status": "success",
            "velocity_knot": first_hour["speed"],
            "direction": first_hour["direction"],
            "source": "HYCOM_CACHE"
        }
    return {
        "status": "loading",
        "message": "HYCOM data not ready or the location is land"
    }

# =========================================================
# API: FORECAST (修正: 毎回通信せず、保存されたキャッシュを即返すだけ)
# =========================================================
@app.get("/forecast")
def forecast(
    lat: float = Query(...),
    lon: float = Query(...)
):
    key = f"{round(lat,2)}_{round(lon,2)}"
    with lock:
        cache = forecast_cache.get(key)

    if cache:
        return {
            "status": "success",
            "data": cache["data"]
        }
        
    return {
        "status": "loading",
        "message": "No cache available. Waiting for daily update.",
        "data": []
    }

# =========================================================
# HYCOM BATCH PROCESS (新設: 1回だけ安全にデータを開いて抽出し、すぐ閉じる)
# =========================================================
def execute_hycom_batch():
    global hycom_ready
    print("HYCOM BATCH PROCESS START", flush=True)
    
    ds = None
    success_count = 0
    temp_forecasts = {}
    
    try:
        # chunks を追加して、巨大データを開いた瞬間のメモリ消費を大幅に節約
        ds = xr.open_dataset(DATA_URL, engine="netcdf4", decode_times=False, chunks={"time": 10}).sel(
            lat=slice(30, 46),
            lon=slice(129, 146)
        )

        for name, lat, lon in HYCOM_POINTS:
            try:
                response = build_forecast_response(ds, lat, lon)
                if response["status"] == "success" and response["data"]:
                    key = f"{round(lat,2)}_{round(lon,2)}"
                    temp_forecasts[key] = {
                        "data": response["data"]
                    }
                    success_count += 1
                    print(f"HYCOM BATCH EXTRACT OK: {name}", flush=True)
            except Exception as item_err:
                print(f"HYCOM BATCH EXTRACT FAIL: {name} {item_err}", flush=True)
            
            # 各都市の間を1秒あけて、時間をかけてゆっくり安全に処理する
            time.sleep(1)

        if success_count > 0:
            with lock:
                for k, v in temp_forecasts.items():
                    forecast_cache[k] = v
                hycom_ready = True

    except Exception as e:
        print(f"HYCOM BATCH CRITICAL ERROR: {e}", flush=True)
    finally:
        # 【超重要】使い終わったデータセットを確実に閉じてRenderのメモリを解放する
        if ds is not None:
            try:
                ds.close()
                print("✅ HYCOM Dataset safely closed and memory freed.", flush=True)
            except Exception:
                pass
                
    return success_count

@app.get("/routes")
def routes():

    return [
        route.path
        for route in app.routes
    ]
# =========================================================
# DAILY CACHE BUILDER
# =========================================================

UMISHIRU_AREAS = [
    "03","01","02","04","05",
    "06","07","08","S01"
]

HYCOM_POINTS = [
    ("goto",32.7,128.7),
    ("amakusa",32.3,130.0),
    ("iki",33.9,129.7),
    ("shimonoseki_off",34.1,130.7),
    ("bungo",33.2,132.0),
    ("miyazaki_off",31.9,131.9),
    ("kochi_off",33.3,133.5),
    ("kii",34.0,134.8),
    ("kushimoto",33.47,135.78),
    ("omaezaki",34.60,138.20),
    ("suruga",34.8,138.6),
    ("sagami",35.0,139.4),
    ("choshi_offshore",35.5,141.0),
    ("iwate",39.6,141.9),
    ("sendai",38.3,141.0),
    ("fukushima",36.9,140.9),
    ("hachinohe",41.4,142.2),
    ("hakodate",41.8,140.7),
    ("hirosaki",41.2,140.2),
    ("akita",39.7,139.9),
    ("niigata",37.9,139.0),
    ("toyama",37.4,137.9),
    ("kanazawa",36.6,136.6),
    ("wakasa",35.9,135.6),
    ("matsue_off",35.9,133.4),
    ("tottori",35.7,134.4),
    ("shimane",34.9,132.1),
    ("hagi",34.5,131.3),
    ("sapporo",43.2,140.9),
    ("rumoi",43.9,141.6),
    ("monbetsu",44.3,143.3),
    ("kushiro",42.9,144.4),
    ("nemuro",43.3,145.9)
]


def build_daily_umishiru():

    print("UMISHIRU DAILY START", flush=True)

    success_count = 0

    for area in UMISHIRU_AREAS:

        try:

            data = fetch_48h_parallel(area)

            if data["status"] != "success":
                continue

            with lock:
                umishiru_cache[area] = {
                    "data": data
                }

            success_count += 1

            print(
                f"UMISHIRU OK {area}",
                flush=True
            )

        except Exception as e:

            print(
                f"UMISHIRU FAIL {area} {e}",
                flush=True
            )

        time.sleep(90)

    return success_count


def build_daily_hycom():

    print("HYCOM DAILY START", flush=True)

            for name, lat, lon in HYCOM_POINTS:
            try:
                response = build_forecast_response(ds, lat, lon)
                if response["status"] == "success" and response["data"]:
                    key = f"{round(lat,2)}_{round(lon,2)}"
                    temp_forecasts[key] = {
                        "data": response["data"]
                    }
                    success_count += 1
                    print(f"HYCOM BATCH EXTRACT OK: {name}", flush=True)
            except Exception as item_err:
                print(f"HYCOM BATCH EXTRACT FAIL: {name} {item_err}", flush=True)
            
            # ★修正：次の地点を取得するまで180秒（3分）待つ
            time.sleep(180)

        if success_count > 0:

# =========================================================
# SCHEDULED TASK & STARTUP (修正: ヘルスチェック落ち対策でバックグラウンド化)
# =========================================================
def scheduled_cache_builder():
    last_umishiru_1am = None
    last_umishiru_3am = None
    last_hycom_day = None

    while True:
        now = datetime.now(JST)
        today = now.strftime("%Y-%m-%d")

        # 01:00 海しる
        if now.hour == 1 and now.minute < 5 and last_umishiru_1am != today:
            if build_daily_umishiru() > 0:
                last_umishiru_1am = today

        # 03:00 海しる再取得
        if now.hour == 3 and now.minute < 5 and last_umishiru_3am != today:
            if build_daily_umishiru() > 0:
                last_umishiru_3am = today

        # 🔄 06:00 HYCOM 毎日更新（成功するまで毎時トライ）
        if now.hour >= 6 and last_hycom_day != today:
            print(f"⏰ Starting HYCOM daily batch attempt at {now.strftime('%H:%M')}", flush=True)
            
            count = execute_hycom_batch()
            
            if count > 0:
                print(f"✅ HYCOM daily batch success! ({count} points cached)", flush=True)
                last_hycom_day = today
            else:
                print("❌ HYCOM daily batch failed completely. Will retry in 1 hour.", flush=True)
                time.sleep(3600)

        time.sleep(120)


# 起動時に別スレッドで安全に最初のキャッシュを作るための関数
def run_initial_hycom_batch():
    print("Background thread: Building initial HYCOM cache...", flush=True)
    for i in range(3):
        count = execute_hycom_batch()
        if count > 0:
            print("✅ Initial HYCOM cache built successfully via background thread.", flush=True)
            break
        print(f"⚠️ retry initial HYCOM batch {i+1}/3", flush=True)
        time.sleep(10)


@app.on_event("startup")
def startup():
    print("🚀 Startup begin", flush=True)

    # ★指摘の修正: 重い初回取得を別スレッドに逃がし、Webサーバー自体は「一瞬」で起動させる
    # これにより Render のヘルスチェックによる強制終了（タイムアウト）を完全に回避します
    threading.Thread(target=run_initial_hycom_batch, daemon=True).start()

    # 定期監視タスクもスレッドで起動
    threading.Thread(target=scheduled_cache_builder, daemon=True).start()
    
    print("✅ Startup complete (API web server is now live and listening)", flush=True)
