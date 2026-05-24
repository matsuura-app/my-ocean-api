from datetime import datetime, timedelta
import socket
import threading

import numpy as np
import requests
import xarray as xr

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# FastAPI
# =========================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# GLOBAL SETTINGS
# =========================================================

socket.setdefaulttimeout(20)

session = requests.Session()

# =========================================================
# UMISHIRU
# =========================================================

UMISHIRU_URL = (
    "https://msil.go.jp/msil/OgpProvider/"
    "czm/current_forecast_grid.aspx"
)

umishiru_cache = {
    "data": None,
    "fetched_at": None
}

umishiru_lock = threading.Lock()

# =========================================================
# NOAA URL CACHE
# =========================================================

noaa_url_cache = {
    "url": None,
    "checked_at": None
}

# =========================================================
# UTIL
# =========================================================

def safe_float(v, default=0.0):
    try:
        if np.ma.is_masked(v):
            return default

        v = float(v)

        if np.isnan(v):
            return default

        if np.isinf(v):
            return default

        return v

    except Exception:
        return default

# =========================================================
# UMISHIRU
# =========================================================

def fetch_umishiru_data():

    with umishiru_lock:

        now = datetime.utcnow()

        if (
            umishiru_cache["data"] is not None
            and umishiru_cache["fetched_at"] is not None
        ):

            if now - umishiru_cache["fetched_at"] < timedelta(minutes=30):

                print(
                    "💾 UMISHIRU CACHE HIT",
                    flush=True
                )

                return umishiru_cache["data"]

        print(
            "🌐 FETCH UMISHIRU",
            flush=True
        )

        try:

            r = session.get(
                UMISHIRU_URL,
                timeout=(5, 10)
            )

            if r.status_code == 200:

                data = r.json()

                umishiru_cache["data"] = data
                umishiru_cache["fetched_at"] = now

                return data

        except Exception as e:

            print(
                f"⚠️ UMISHIRU ERROR: {e}",
                flush=True
            )

        return umishiru_cache["data"]

def find_nearest_umishiru(
    lat: float,
    lon: float,
    data: dict
):

    if not data:
        return None

    features = data.get("features")

    if not features:
        return None

    nearest = None
    min_dist = float("inf")

    for feature in features:

        try:

            geom = feature.get("geometry", {})

            if geom.get("type") != "Point":
                continue

            coords = geom.get("coordinates", [])

            if len(coords) < 2:
                continue

            m_lon = float(coords[0])
            m_lat = float(coords[1])

            dist = (
                (lat - m_lat) ** 2
                + (lon - m_lon) ** 2
            )

            if dist < min_dist:
                min_dist = dist
                nearest = feature

        except Exception:
            continue

    # 約15km
    if min_dist > 0.02:
        return None

    return nearest

# =========================================================
# NOAA
# =========================================================

def get_active_rtofs_url():

    now = datetime.utcnow()

    if (
        noaa_url_cache["url"]
        and noaa_url_cache["checked_at"]
    ):

        if (
            now - noaa_url_cache["checked_at"]
            < timedelta(hours=3)
        ):

            print(
                "💾 NOAA URL CACHE HIT",
                flush=True
            )

            return noaa_url_cache["url"]

    base_url = "https://nomads.ncep.noaa.gov/dods/rtofs"

    patterns = [
        "rtofs_glo_2ds_f006_3hrly_prog",
        "rtofs_glo_2ds_forecast_3hrly_prog"
    ]

    for days_back in [0, 1, 2]:

        target = now - timedelta(days=days_back)

        date_str = target.strftime("%Y%m%d")

        for pattern in patterns:

            url = (
                f"{base_url}/"
                f"rtofs{date_str}/"
                f"{pattern}"
            )

            dds_url = f"{url}.dds"

            print(
                f"🔍 NOAA CHECK: {dds_url}",
                flush=True
            )

            try:

                r = session.get(
                    dds_url,
                    timeout=(5, 10)
                )

                if r.status_code == 200:

                    print(
                        f"🚀 NOAA OK: {url}",
                        flush=True
                    )

                    noaa_url_cache["url"] = url
                    noaa_url_cache["checked_at"] = now

                    return url

            except Exception as e:

                print(
                    f"⚠️ NOAA FAIL: {e}",
                    flush=True
                )

    return None

# =========================================================
# NOAA FORECAST
# =========================================================

def generate_noaa_forecast(
    lat: float,
    lon: float
):

    if lon < 0:
        lon += 360.0

    active_url = get_active_rtofs_url()

    if not active_url:

        raise HTTPException(
            status_code=503,
            detail="NOAA URL unavailable"
        )

    ds = None

    try:

        print(
            f"📦 OPEN DATASET: {active_url}",
            flush=True
        )

        ds = xr.open_dataset(
            active_url,
            engine="pydap",
            decode_times=False,
            decode_cf=False,
            cache=False
        )

        lat_key = (
            "lat"
            if "lat" in ds.coords
            else "latitude"
        )

        lon_key = (
            "lon"
            if "lon" in ds.coords
            else "longitude"
        )

        point = ds.sel(
            {
                lat_key: lat,
                lon_key: lon
            },
            method="nearest"
        )

        u_var = None
        v_var = None

        for candidate in [
            "u",
            "u_velocity",
            "u_current"
        ]:
            if candidate in ds.data_vars:
                u_var = candidate
                break

        for candidate in [
            "v",
            "v_velocity",
            "v_current"
        ]:
            if candidate in ds.data_vars:
                v_var = candidate
                break

        if not u_var or not v_var:

            raise HTTPException(
                status_code=500,
                detail="Current variables not found"
            )

        time_size = ds.sizes.get("time", 0)

        results = []

        for h in range(min(16, time_size)):

            try:

                subset = point.isel(time=h)

                u = safe_float(
                    subset[u_var].values.item()
                )

                v = safe_float(
                    subset[v_var].values.item()
                )

                if abs(u) > 99 or abs(v) > 99:

                    results.append({
                        "time": h * 3,
                        "speed": 0.0,
                        "direction": 0.0
                    })

                    continue

                speed_ms = np.sqrt(
                    u ** 2 + v ** 2
                )

                speed_knots = (
                    speed_ms * 1.94384
                )

                direction = np.degrees(
                    np.arctan2(u, v)
                )

                if direction < 0:
                    direction += 360

                results.append({
                    "time": h * 3,
                    "speed": round(
                        float(speed_knots),
                        2
                    ),
                    "direction": round(
                        float(direction),
                        1
                    )
                })

            except Exception as e:

                print(
                    f"⚠️ FORECAST ITEM ERROR: {e}",
                    flush=True
                )

                results.append({
                    "time": h * 3,
                    "speed": 0.0,
                    "direction": 0.0
                })

        return results

    except HTTPException:
        raise

    except Exception as e:

        print(
            f"❌ NOAA ERROR: {e}",
            flush=True
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:

        try:
            if ds is not None:
                ds.close()
        except Exception:
            pass

# =========================================================
# ROOT
# =========================================================

@app.get("/")
def root():

    return {
        "status": "ok",
        "server": "hybrid-v3"
    }

# =========================================================
# FORECAST
# =========================================================

@app.get("/forecast")
def forecast(
    lat: float = Query(...),
    lon: float = Query(...)
):

    print(
        f"📡 REQUEST lat={lat} lon={lon}",
        flush=True
    )

    # =====================================================
    # UMISHIRU FIRST
    # =====================================================

    try:

        umishiru_data = fetch_umishiru_data()

        if umishiru_data:

            nearest = find_nearest_umishiru(
                lat,
                lon,
                umishiru_data
            )

            if nearest:

                props = nearest.get(
                    "properties",
                    {}
                )

                print(
                    "🎯 USING UMISHIRU",
                    flush=True
                )

                return {
                    "status": "success",
                    "source": "umishiru",
                    "lat": lat,
                    "lon": lon,
                    "data": [
                        {
                            "time": 0,
                            "speed": safe_float(
                                props.get("speed", 0)
                            ),
                            "direction": safe_float(
                                props.get("direction", 0)
                            )
                        }
                    ]
                }

    except Exception as e:

        print(
            f"⚠️ UMISHIRU FAILED: {e}",
            flush=True
        )

    # =====================================================
    # NOAA FALLBACK
    # =====================================================

    print(
        "🌐 USING NOAA RTOFS",
        flush=True
    )

    noaa_data = generate_noaa_forecast(
        lat,
        lon
    )

    return {
        "status": "success",
        "source": "noaa_rtofs",
        "lat": lat,
        "lon": lon,
        "data": noaa_data
    }
