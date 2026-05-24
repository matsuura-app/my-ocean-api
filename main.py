import os
import socket
from datetime import datetime, timedelta
import numpy as np
import xarray as xr
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# アドバイス③：グローバルでのタイムアウトを設定してハングを徹底防御
socket.setdefaulttimeout(20)

app = FastAPI()

# CORS設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 海しる用設定 ---
UMISHIRU_URL = "https://msil.go.jp/msil/OgpProvider/czm/current_forecast_grid.aspx"
umishiru_cache = {"data": None, "fetched_at": None}

def fetch_umishiru_data():
    now = datetime.now()
    if umishiru_cache["data"] and umishiru_cache["fetched_at"]:
        if now - umishiru_cache["fetched_at"] < timedelta(minutes=30):
            print("💾 [海しる] キャッシュからデータを返却します", flush=True)
            return umishiru_cache["data"]

    print("🌐 [海しる] 海上保安庁サーバーから新規データを取りに行きます...", flush=True)
    try:
        response = requests.get(UMISHIRU_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            umishiru_cache["data"] = data
            umishiru_cache["fetched_at"] = now
            return data
    except Exception as e:
        print(f"⚠️ [海しる] データ取得失敗: {e}", flush=True)
    return umishiru_cache["data"]

def find_nearest_umishiru(lat: float, lon: float, data: dict):
    if not data or "features" not in data:
        return None
    nearest_feature = None
    min_dist = float("inf")
    for feature in data["features"]:
        geom = feature.get("geometry", {})
        if geom.get("type") == "Point":
            coords = geom.get("coordinates", [])
            if len(coords) >= 2:
                m_lon, m_lat = coords[0], coords[1]
                dist = (lat - m_lat) ** 2 + (lon - m_lon) ** 2
                if dist < min_dist:
                    min_dist = dist
                    nearest_feature = feature
    if min_dist > 0.02: # 約15km
        return None
    return nearest_feature


# --- NOAA用設定（アドバイスを100%反映した修正版） ---
def get_active_rtofs_url():
    """
    アドバイスに従い、ポート :9090 を完全に廃止し、
    安全な『https://.../dods/rtofs/rtofs...』のURL構造でチェックします。
    """
    # 修正：:9090を消し、dodsを含めた最新の正しいベースURLに固定
    base_url = "https://nomads.ncep.noaa.gov/dods/rtofs"
    
    # アドバイス④：最近安定しているパターンを優先
    dataset_patterns = [
        "rtofs_glo_2ds_f006_3hrly_prog",
        "rtofs_glo_2ds_forecast_3hrly_prog"
    ]
    
    now_utc = datetime.utcnow()
    
    for days_back in [0, 1, 2]:
        target_date = now_utc - timedelta(days=days_back)
        date_str = target_date.strftime("%Y%m%d")
        
        for pattern in dataset_patterns:
            # 正しいURL組み立て
            url = f"{base_url}/rtofs{date_str}/{pattern}"
            dds_url = url + ".dds"
            
            print(f"🔍 NOAA URL確認中(修正版): {dds_url}", flush=True)
            try:
                # タイムアウトを設けて軽量な.ddsだけを取得
                r = requests.get(dds_url, timeout=7.0)
                if r.status_code == 200:
                    print(f"🚀 NOAA URL OK (接続成功!): {url}", flush=True)
                    return url
                else:
                    print(f"⚠️ NOAA URL NG (Status {r.status_code}): {url}", flush=True)
            except Exception as e:
                print(f"⚠️ NOAA URL NG (エラー): {str(e)}", flush=True)
                continue
                
    return None

def generate_noaa_forecast(lat: float, lon: float):
    if lon < 0:
        lon += 360.0

    active_url = get_active_rtofs_url()
    if not active_url:
        raise HTTPException(
            status_code=503,
            detail="NOAA RTOFS サーバーの有効なURLが見つかりません。HTTPS通信、またはNOAA側のデータが未生成の可能性があります。"
        )

    # アドバイス②：engine="pydap", decode_times=False を指定して安定化
    try:
        print(f"📦 xarrayでNOAAデータ展開 (engine=pydap): {active_url}", flush=True)
        ds = xr.open_dataset(active_url, engine="pydap", decode_times=False)
    except Exception as e:
        print(f"❌ pydap展開エラー: {str(e)}", flush=True)
        raise HTTPException(status_code=500, detail=f"NOAAデータ解析エラー: {str(e)}")

    try:
        lat_key = "lat" if "lat" in ds.coords else "latitude" if "latitude" in ds.coords else "lat"
        lon_key = "lon" if "lon" in ds.coords else "longitude" if "longitude" in ds.coords else "lon"
        point = ds.sel({lat_key: lat, lon_key: lon}, method="nearest")

        u_var = "u" if "u" in ds.data_vars else "u_velocity" if "u_velocity" in ds.data_vars else None
        v_var = "v" if "v" in ds.data_vars else "v_velocity" if "v_velocity" in ds.data_vars else None

        if not u_var or not v_var:
            raise ValueError("流速データ(u, v)が見つかりません。")

        results = []
        for h in range(16): # 48時間分
            try:
                subset = point.isel(time=h)
                
                # 最優先アドバイス：.item() を使って純粋なfloatにする
                u_val = float(subset[u_var].values.item())
                v_val = float(subset[v_var].values.item())

                if np.isnan(u_val) or np.isnan(v_val) or abs(u_val) > 99.0 or abs(v_val) > 99.0:
                    results.append({"time": h * 3, "speed": 0.0, "direction": 0.0})
                    continue

                speed_ms = np.sqrt(u_val**2 + v_val**2)
                speed_knots = float(speed_ms * 1.94384)
                
                direction = float(np.degrees(np.arctan2(u_val, v_val)))
                if direction < 0:
                    direction += 360.0

                results.append({
                    "time": h * 3,
                    "speed": round(speed_knots, 2),
                    "direction": round(direction, 1)
                })
            except Exception:
                results.append({"time": h * 3, "speed": 0.0, "direction": 0.0})

        return results
    finally:
        ds.close()

# --- メインエンドポイント ---
@app.get("/")
def read_root():
    return {"Status": "ok", "mode": "2026-hybrid-fixed-v2"}

@app.get("/forecast")
def forecast(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude")
):
    print(f"📡 潮流リクエスト受信: lat={lat}, lon={lon}", flush=True)

    # 【第1のコース】日本近海なら「海しる」を最優先
    umishiru_raw = fetch_umishiru_data()
    if umishiru_raw:
        match = find_nearest_umishiru(lat, lon, umishiru_raw)
        if match:
            print("🎯 [海しる] エリア内のため、海上保安庁のデータを採用します", flush=True)
            props = match.get("properties", {})
            return {
                "status": "success",
                "source": "umishiru",
                "lat": lat,
                "lon": lon,
                "data": [{
                    "time": 0,
                    "speed": props.get("speed", 0.0),
                    "direction": props.get("direction", 0.0)
                }]
            }

    # 【第2のコース】エリア外なら、修正された「NOAA」へ通信
    print("🌐 [NOAA] 海しるエリア外（または取得失敗）のため、修正版NOAA予測に切り替えます", flush=True)
    noaa_data = generate_noaa_forecast(lat, lon)
    
    return {
        "status": "success",
        "source": "noaa_rtofs",
        "lat": lat,
        "lon": lon,
        "data": noaa_data
    }
