"""
US Licensed Companies & Regulated Facilities Map
Data sources:
  1. FDIC  – FDIC-insured banks & savings institutions  (~4,300)
  2. SEC   – Publicly-traded companies (10-K filers)     (~10,000)
  3. EPA   – EPA-regulated facilities per state           (~millions, loaded on demand)
"""

import csv
import io
import asyncio
import math
from collections import defaultdict
from pathlib import Path

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="US Licensed Company & Facility Map")

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

STATE_ABBR_TO_NAME = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}

# ── Normalized industry names ────────────────────────────────────────────────
# Both SIC and NAICS map into this unified set of ~15 industries
INDUSTRY_NAMES = {
    "agriculture":     "Agriculture & Forestry",
    "mining":          "Mining & Extraction",
    "utilities":       "Utilities",
    "construction":    "Construction",
    "manufacturing":   "Manufacturing",
    "wholesale":       "Wholesale Trade",
    "retail":          "Retail Trade",
    "transportation":  "Transportation & Warehousing",
    "information":     "Information & Media",
    "finance":         "Finance & Insurance",
    "real_estate":     "Real Estate",
    "professional":    "Professional & Technical Services",
    "management":      "Management of Companies",
    "admin":           "Administrative & Waste Services",
    "education":       "Educational Services",
    "health":          "Health Care & Social Assistance",
    "arts":            "Arts & Entertainment",
    "food":            "Accommodation & Food Services",
    "other_services":  "Other Services",
    "public_admin":    "Public Administration",
    "other":           "Other",
}

SIC_TO_NORMALIZED = {
    range(100, 1000):   "agriculture",
    range(1000, 1500):  "mining",
    range(1500, 1800):  "construction",
    range(2000, 4000):  "manufacturing",
    range(4000, 4900):  "transportation",
    range(4900, 5000):  "utilities",
    range(5000, 5200):  "wholesale",
    range(5200, 6000):  "retail",
    range(6000, 6600):  "finance",
    range(6500, 6600):  "real_estate",
    range(6600, 6800):  "finance",
    range(7000, 9000):  "other_services",
    range(9000, 10000): "public_admin",
}

NAICS_TO_NORMALIZED = {
    "11": "agriculture",
    "21": "mining",
    "22": "utilities",
    "23": "construction",
    "31": "manufacturing", "32": "manufacturing", "33": "manufacturing",
    "42": "wholesale",
    "44": "retail", "45": "retail",
    "48": "transportation", "49": "transportation",
    "51": "information",
    "52": "finance",
    "53": "real_estate",
    "54": "professional",
    "55": "management",
    "56": "admin",
    "61": "education",
    "62": "health",
    "71": "arts",
    "72": "food",
    "81": "other_services",
    "92": "public_admin",
}


def sic_to_industry(sic_str: str) -> str:
    try:
        code = int(sic_str)
    except (TypeError, ValueError):
        return INDUSTRY_NAMES["other"]
    for rng, key in SIC_TO_NORMALIZED.items():
        if code in rng:
            return INDUSTRY_NAMES[key]
    return INDUSTRY_NAMES["other"]


def naics_to_industry(naics_str: str) -> str:
    if not naics_str:
        return ""
    code = naics_str.strip().split()[0][:2]
    key = NAICS_TO_NORMALIZED.get(code, "other")
    return INDUSTRY_NAMES[key]


# ══════════════════════════════════════════════════════════════════════════════
#  FDIC  –  Banks & savings institutions
# ══════════════════════════════════════════════════════════════════════════════
FDIC_API = "https://api.fdic.gov/banks"
FDIC_FIELDS = "NAME,ADDRESS,CITY,STNAME,ZIP,LATITUDE,LONGITUDE,BKCLASS,SPECGRPN,ESTYMD,ASSET,CERT"

_fdic_cache: list[dict] = []


async def load_fdic() -> list[dict]:
    global _fdic_cache
    if _fdic_cache:
        return _fdic_cache

    results = []
    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        offset = 0
        while True:
            resp = await client.get(f"{FDIC_API}/institutions", params={
                "filters": "ACTIVE:1", "fields": FDIC_FIELDS,
                "limit": 10000, "offset": offset,
            })
            resp.raise_for_status()
            page = resp.json().get("data", [])
            if not page:
                break
            results.extend(page)
            if len(page) < 10000:
                break
            offset += 10000

    for row in results:
        d = row.get("data", {})
        try:
            lat, lon = float(d["LATITUDE"]), float(d["LONGITUDE"])
        except (TypeError, ValueError, KeyError):
            continue
        if lat == 0 and lon == 0:
            continue
        _fdic_cache.append({
            "source": "fdic",
            "name": d.get("NAME", ""),
            "address": d.get("ADDRESS", ""),
            "city": d.get("CITY", ""),
            "state": d.get("STNAME", ""),
            "zip": d.get("ZIP", ""),
            "lat": lat, "lon": lon,
            "industry": INDUSTRY_NAMES["finance"],
            "detail": d.get("SPECGRPN", ""),
            "sic": "", "naics": "",
            "extra": {
                "assets": d.get("ASSET"),
                "cert": d.get("CERT"),
                "class": d.get("BKCLASS", ""),
                "established": d.get("ESTYMD", ""),
            },
        })

    print(f"[FDIC] Loaded {len(_fdic_cache)} banks")
    return _fdic_cache


# ══════════════════════════════════════════════════════════════════════════════
#  SEC EDGAR  –  Publicly-traded companies
# ══════════════════════════════════════════════════════════════════════════════
SEC_UA = "ScamDetector/1.0 research@example.com"

_sec_cache: list[dict] = []


async def load_sec() -> list[dict]:
    global _sec_cache
    if _sec_cache:
        return _sec_cache

    async with httpx.AsyncClient(follow_redirects=True, timeout=60, headers={"User-Agent": SEC_UA}) as client:
        resp = await client.get("https://www.sec.gov/files/company_tickers_exchange.json")
        resp.raise_for_status()
        bulk = resp.json()
        companies = bulk["data"]
        print(f"[SEC] Found {len(companies)} tickers, fetching addresses...")

        cik_map = {}
        for row in companies:
            cik_map[int(row[0])] = {"name": row[1], "ticker": row[2], "exchange": row[3]}

        sem = asyncio.Semaphore(8)

        async def fetch_one(cik: int) -> dict | None:
            async with sem:
                url = f"https://data.sec.gov/submissions/CIK{cik:010d}.json"
                try:
                    r = await client.get(url)
                    if r.status_code != 200:
                        return None
                    d = r.json()
                    addr = d.get("addresses", {}).get("business", {})
                    sic = d.get("sic", "")
                    state_code = addr.get("stateOrCountry", "")
                    city = addr.get("city", "")
                    zipcode = addr.get("zipCode", "")
                    if not city or not state_code or len(state_code) != 2:
                        return None
                    info = cik_map[cik]
                    return {
                        "source": "sec",
                        "name": info["name"],
                        "address": f"{addr.get('street1', '')} {addr.get('street2', '') or ''}".strip(),
                        "city": city,
                        "state": STATE_ABBR_TO_NAME.get(state_code, state_code),
                        "zip": zipcode,
                        "lat": 0, "lon": 0,
                        "industry": sic_to_industry(sic),
                        "detail": d.get("sicDescription", ""),
                        "sic": sic, "naics": "",
                        "extra": {
                            "ticker": info["ticker"],
                            "exchange": info["exchange"],
                            "cik": cik,
                            "sic_desc": d.get("sicDescription", ""),
                        },
                    }
                except Exception:
                    return None

        tasks = [fetch_one(cik) for cik in cik_map]
        results = await asyncio.gather(*tasks)
        _sec_cache = [r for r in results if r is not None]
        print(f"[SEC] Loaded {len(_sec_cache)} companies with addresses")
        return _sec_cache


# ══════════════════════════════════════════════════════════════════════════════
#  EPA ECHO  –  Regulated facilities (loaded per-state on demand)
#  Uses p_act=Y (active only) and bounding-box splits for huge states.
# ══════════════════════════════════════════════════════════════════════════════
EPA_ECHO = "https://echodata.epa.gov/echo/echo_rest_services"
EPA_QCOLS = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18"

_epa_cache: dict[str, list[dict]] = {}

# Bounding-box splits for states whose active-facility count exceeds the API limit.
# Each entry is a list of (lat_min, lon_min, lat_max, lon_max).
EPA_LARGE_STATE_SPLITS: dict[str, list[tuple[float, float, float, float]]] = {
    "CA": [
        (32.0, -125.0, 34.0, -114.0),   # SoCal south
        (34.0, -125.0, 36.0, -114.0),   # SoCal north / Central
        (36.0, -125.0, 42.5, -114.0),   # NorCal
    ],
}


def _parse_epa_csv(text: str, state_abbr: str) -> list[dict]:
    """Parse EPA CSV download into normalized dicts."""
    facilities = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        try:
            lat = float(row.get("FacLat", "") or "0")
            lon = float(row.get("FacLong", "") or "0")
        except ValueError:
            continue
        if lat == 0 or lon == 0:
            continue

        sic_raw = (row.get("FacSICCodes") or "").strip()
        naics_raw = (row.get("FacNAICSCodes") or "").strip()
        if sic_raw:
            industry = sic_to_industry(sic_raw.split()[0])
        elif naics_raw:
            industry = naics_to_industry(naics_raw)
        else:
            industry = INDUSTRY_NAMES["other"]

        facilities.append({
            "source": "epa",
            "name": (row.get("FacName") or "").strip(),
            "address": (row.get("FacStreet") or "").strip(),
            "city": (row.get("FacCity") or "").strip(),
            "state": STATE_ABBR_TO_NAME.get(state_abbr, state_abbr),
            "zip": (row.get("FacZip") or "").strip(),
            "lat": lat, "lon": lon,
            "industry": industry,
            "detail": sic_raw or naics_raw or "",
            "sic": sic_raw, "naics": naics_raw,
            "extra": {
                "registry_id": row.get("RegistryID", ""),
                "county": (row.get("FacCounty") or "").strip(),
                "federal": row.get("FacFederalFlg", ""),
                "epa_region": row.get("FacEPARegion", ""),
            },
        })
    return facilities


async def _epa_query_and_download(
    client: httpx.AsyncClient,
    state_abbr: str,
    extra_params: dict | None = None,
) -> list[dict]:
    """Run one EPA query → download cycle. Returns parsed facilities."""
    params: dict = {"output": "JSON", "p_st": state_abbr, "p_act": "Y"}
    if extra_params:
        params.update(extra_params)

    resp = await client.get(f"{EPA_ECHO}.get_facilities", params=params)
    resp.raise_for_status()
    result = resp.json().get("Results", {})

    # Check for errors (query too large, etc.)
    if "Error" in result:
        raise ValueError(result["Error"].get("ErrorMessage", "Unknown EPA error"))

    qid = result.get("QueryID")
    if not qid:
        raise ValueError("No QueryID returned from EPA")

    query_rows = int(result.get("QueryRows", 0))
    if query_rows == 0:
        return []

    # Download CSV
    resp2 = await client.get(f"{EPA_ECHO}.get_download", params={
        "qid": qid, "qcolumns": EPA_QCOLS,
    })
    resp2.raise_for_status()
    return _parse_epa_csv(resp2.text, state_abbr)


async def load_epa_state(state_abbr: str) -> list[dict]:
    state_abbr = state_abbr.upper()
    if state_abbr in _epa_cache:
        return _epa_cache[state_abbr]

    facilities: list[dict] = []
    async with httpx.AsyncClient(follow_redirects=True, timeout=300) as client:
        if state_abbr in EPA_LARGE_STATE_SPLITS:
            # Split into bounding-box sub-queries
            splits = EPA_LARGE_STATE_SPLITS[state_abbr]
            seen_ids: set[str] = set()
            for lat_min, lon_min, lat_max, lon_max in splits:
                try:
                    chunk = await _epa_query_and_download(client, state_abbr, {
                        "p_c1lat": str(lat_min), "p_c1lon": str(lon_min),
                        "p_c2lat": str(lat_max), "p_c2lon": str(lon_max),
                    })
                    # Deduplicate across splits
                    for f in chunk:
                        rid = f["extra"].get("registry_id", "")
                        if rid and rid in seen_ids:
                            continue
                        if rid:
                            seen_ids.add(rid)
                        facilities.append(f)
                    print(f"  [EPA] {state_abbr} bbox ({lat_min}-{lat_max}): {len(chunk)} facilities")
                except Exception as e:
                    print(f"  [EPA] {state_abbr} bbox ({lat_min}-{lat_max}) failed: {e}")
        else:
            try:
                facilities = await _epa_query_and_download(client, state_abbr)
            except ValueError as e:
                err_msg = str(e)
                if "exceeded" in err_msg.lower():
                    # Fallback: try with bounding box split (north/south halves)
                    print(f"  [EPA] {state_abbr} too large, splitting N/S...")
                    for lat_min, lat_max in [(24.0, 37.0), (37.0, 50.0)]:
                        try:
                            chunk = await _epa_query_and_download(client, state_abbr, {
                                "p_c1lat": str(lat_min), "p_c1lon": "-130",
                                "p_c2lat": str(lat_max), "p_c2lon": "-60",
                            })
                            facilities.extend(chunk)
                            print(f"  [EPA] {state_abbr} lat {lat_min}-{lat_max}: {len(chunk)}")
                        except Exception as e2:
                            print(f"  [EPA] {state_abbr} lat {lat_min}-{lat_max} failed: {e2}")
                else:
                    raise

    _epa_cache[state_abbr] = facilities
    print(f"[EPA] Loaded {len(facilities)} facilities for {state_abbr}")
    return facilities


# ══════════════════════════════════════════════════════════════════════════════
#  ZIP-code centroid geocoding (for SEC data that lacks lat/lon)
# ══════════════════════════════════════════════════════════════════════════════
_zip_centroids: dict[str, tuple[float, float]] = {}


def _rebuild_zip_centroids():
    """Rebuild zip→(lat,lon) map from all loaded geocoded data."""
    global _zip_centroids
    _zip_centroids.clear()
    for src in [_fdic_cache] + list(_epa_cache.values()):
        for item in src:
            z = item.get("zip", "")[:5]
            if z and item["lat"] != 0 and item["lon"] != 0:
                if z not in _zip_centroids:
                    _zip_centroids[z] = (item["lat"], item["lon"])


def geocode_from_zip(item: dict) -> bool:
    z = item.get("zip", "")[:5]
    if z in _zip_centroids:
        item["lat"], item["lon"] = _zip_centroids[z]
        return True
    return False


def _geocode_sec_entries():
    """Try to geocode all SEC entries that still lack coordinates."""
    _rebuild_zip_centroids()
    count = 0
    for item in _sec_cache:
        if item["lat"] == 0 and geocode_from_zip(item):
            count += 1
    if count:
        print(f"[GEO] Geocoded {count} SEC entries ({len(_zip_centroids)} zip codes available)")


# ══════════════════════════════════════════════════════════════════════════════
#  Loading progress tracking for "load all"
# ══════════════════════════════════════════════════════════════════════════════
_load_all_progress: dict = {"running": False, "done": 0, "total": 0, "current": "", "errors": []}


# ══════════════════════════════════════════════════════════════════════════════
#  API Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/sources")
async def get_sources():
    return {
        "sources": [
            {"id": "fdic", "name": "FDIC Banks", "description": "FDIC-insured banks & savings institutions", "count": len(_fdic_cache), "loaded": bool(_fdic_cache)},
            {"id": "sec",  "name": "SEC Public Companies", "description": "Publicly-traded companies (SEC EDGAR)", "count": len(_sec_cache), "loaded": bool(_sec_cache)},
            {"id": "epa",  "name": "EPA Facilities", "description": "EPA-regulated facilities (per state)", "count": sum(len(v) for v in _epa_cache.values()), "loaded_states": sorted(_epa_cache.keys())},
        ]
    }


@app.get("/api/load/{source}")
async def load_source(source: str, state: str | None = Query(None)):
    try:
        if source == "fdic":
            data = await load_fdic()
            _geocode_sec_entries()
            return {"count": len(data), "message": f"Loaded {len(data)} FDIC banks"}
        elif source == "sec":
            data = await load_sec()
            _geocode_sec_entries()
            geocoded = sum(1 for item in _sec_cache if item["lat"] != 0)
            return {"count": len(data), "message": f"Loaded {len(data)} SEC companies ({geocoded} geocoded)"}
        elif source == "epa":
            if not state:
                return {"error": "EPA requires a state parameter (2-letter code)"}
            data = await load_epa_state(state)
            _geocode_sec_entries()
            return {"count": len(data), "message": f"Loaded {len(data)} EPA facilities for {state}"}
        return {"error": f"Unknown source: {source}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/load-all-epa")
async def load_all_epa():
    """Load EPA data for all 51 states sequentially. Returns progress info."""
    global _load_all_progress
    if _load_all_progress["running"]:
        return {"status": "already_running", "progress": _load_all_progress}

    remaining = [s for s in US_STATES if s not in _epa_cache]
    if not remaining:
        return {"status": "complete", "message": "All states already loaded",
                "total": sum(len(v) for v in _epa_cache.values())}

    _load_all_progress = {"running": True, "done": 0, "total": len(remaining), "current": "", "errors": []}

    async def run():
        for st in remaining:
            _load_all_progress["current"] = st
            try:
                await load_epa_state(st)
            except Exception as e:
                _load_all_progress["errors"].append(f"{st}: {e}")
                print(f"[EPA] Error loading {st}: {e}")
            _load_all_progress["done"] += 1
        _load_all_progress["running"] = False
        _load_all_progress["current"] = ""
        _geocode_sec_entries()
        print(f"[EPA] All states loaded: {sum(len(v) for v in _epa_cache.values())} total facilities")

    asyncio.create_task(run())
    return {"status": "started", "remaining": len(remaining),
            "message": f"Loading {len(remaining)} states in background..."}


@app.get("/api/load-all-progress")
async def load_all_progress():
    total_facilities = sum(len(v) for v in _epa_cache.values())
    return {
        **_load_all_progress,
        "loaded_states": sorted(_epa_cache.keys()),
        "total_facilities": total_facilities,
    }


@app.get("/api/data")
async def get_data(
    sources: str = Query("fdic", description="Comma-separated: fdic,sec,epa"),
    state: str | None = Query(None),
    industry: str | None = Query(None),
    search: str | None = Query(None),
):
    all_data: list[dict] = []
    src_list = [s.strip().lower() for s in sources.split(",")]

    if "fdic" in src_list:
        all_data.extend(_fdic_cache)
    if "sec" in src_list:
        all_data.extend(item for item in _sec_cache if item["lat"] != 0)
    if "epa" in src_list:
        for state_data in _epa_cache.values():
            all_data.extend(state_data)

    if state:
        state_l = state.lower()
        all_data = [d for d in all_data if d["state"].lower() == state_l]
    if industry:
        ind_l = industry.lower()
        all_data = [d for d in all_data if d["industry"].lower() == ind_l]
    if search:
        s_l = search.lower()
        all_data = [d for d in all_data if s_l in d["name"].lower()
                    or s_l in d.get("detail", "").lower()]

    return {"count": len(all_data), "data": all_data}


@app.get("/api/filters")
async def get_filters():
    all_data: list[dict] = list(_fdic_cache)
    all_data.extend(item for item in _sec_cache if item["lat"] != 0)
    for v in _epa_cache.values():
        all_data.extend(v)

    states = sorted({d["state"] for d in all_data if d["state"]})
    industries = sorted({d["industry"] for d in all_data if d["industry"]})
    sources_loaded = []
    if _fdic_cache:
        sources_loaded.append("fdic")
    if _sec_cache:
        sources_loaded.append("sec")
    if _epa_cache:
        sources_loaded.append("epa")

    return {
        "states": states,
        "industries": industries,
        "sources": sources_loaded,
        "epa_loaded_states": sorted(_epa_cache.keys()),
        "us_states": [{"abbr": k, "name": v} for k, v in sorted(STATE_ABBR_TO_NAME.items(), key=lambda x: x[1])],
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Cluster Analysis  –  Find geographic concentrations of same-industry entities
# ══════════════════════════════════════════════════════════════════════════════

def _gather_all_data(sources: str) -> list[dict]:
    """Collect all loaded data from the requested sources."""
    all_data: list[dict] = []
    src_list = [s.strip().lower() for s in sources.split(",")]
    if "fdic" in src_list:
        all_data.extend(_fdic_cache)
    if "sec" in src_list:
        all_data.extend(item for item in _sec_cache if item["lat"] != 0)
    if "epa" in src_list:
        for state_data in _epa_cache.values():
            all_data.extend(state_data)
    return all_data


def _grid_key(lat: float, lon: float, cell_deg: float) -> tuple[int, int]:
    """Return the grid cell index for a coordinate."""
    return (int(math.floor(lat / cell_deg)), int(math.floor(lon / cell_deg)))


def _cell_center(row: int, col: int, cell_deg: float) -> tuple[float, float]:
    return ((row + 0.5) * cell_deg, (col + 0.5) * cell_deg)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@app.get("/api/analyze")
async def analyze_clusters(
    sources: str = Query("fdic,sec,epa"),
    radius_km: float = Query(5.0, description="Search radius in km (controls grid cell size)"),
    min_companies: int = Query(5, description="Minimum companies in a cluster to report"),
    industry: str | None = Query(None, description="Filter to specific industry"),
    state: str | None = Query(None, description="Filter to specific state"),
    search: str | None = Query(None, description="Search within company names"),
    top_n: int = Query(100, description="Return top N clusters"),
):
    """
    Find geographic clusters of companies with the same industry.
    Groups nearby companies (within radius_km) by industry, ranks by density.
    """
    all_data = _gather_all_data(sources)

    # Apply pre-filters
    if state:
        state_l = state.lower()
        all_data = [d for d in all_data if d["state"].lower() == state_l]
    if search:
        s_l = search.lower()
        all_data = [d for d in all_data if s_l in d["name"].lower()
                    or s_l in d.get("detail", "").lower()]

    if not all_data:
        return {"clusters": [], "total_analyzed": 0}

    # Convert radius to approximate degrees for grid cell size
    # 1 degree latitude ≈ 111 km
    cell_deg = max(radius_km / 111.0, 0.005)

    # Group by (industry, grid_cell)
    grid: dict[tuple[str, int, int], list[dict]] = defaultdict(list)
    for item in all_data:
        ind = item["industry"]
        if industry and ind.lower() != industry.lower():
            continue
        r, c = _grid_key(item["lat"], item["lon"], cell_deg)
        grid[(ind, r, c)].append(item)

    # Build cluster results
    clusters = []
    for (ind, r, c), members in grid.items():
        if len(members) < min_companies:
            continue

        # Compute centroid
        avg_lat = sum(m["lat"] for m in members) / len(members)
        avg_lon = sum(m["lon"] for m in members) / len(members)

        # Compute actual spread (max distance from centroid)
        max_dist = 0.0
        for m in members:
            d = _haversine_km(avg_lat, avg_lon, m["lat"], m["lon"])
            if d > max_dist:
                max_dist = d

        # Count distinct companies (by name)
        unique_names = {m["name"].upper().strip() for m in members if m["name"].strip()}

        # Most common company names (potential duplicates / chains)
        name_counts: dict[str, int] = defaultdict(int)
        for m in members:
            n = m["name"].strip().upper()
            if n:
                name_counts[n] += 1
        top_names = sorted(name_counts.items(), key=lambda x: -x[1])[:10]

        # Source breakdown
        source_counts: dict[str, int] = defaultdict(int)
        for m in members:
            source_counts[m["source"]] += 1

        # City breakdown
        city_counts: dict[str, int] = defaultdict(int)
        for m in members:
            city = m.get("city", "").strip()
            if city:
                city_counts[city] += 1
        top_cities = sorted(city_counts.items(), key=lambda x: -x[1])[:5]

        # State (most common)
        state_counts: dict[str, int] = defaultdict(int)
        for m in members:
            s = m.get("state", "")
            if s:
                state_counts[s] += 1
        primary_state = max(state_counts, key=state_counts.get) if state_counts else ""

        clusters.append({
            "industry": ind,
            "count": len(members),
            "unique_names": len(unique_names),
            "lat": round(avg_lat, 5),
            "lon": round(avg_lon, 5),
            "radius_km": round(max_dist, 2),
            "state": primary_state,
            "top_cities": [{"city": c, "count": n} for c, n in top_cities],
            "top_names": [{"name": n.title(), "count": c} for n, c in top_names],
            "sources": dict(source_counts),
            "density_score": round(len(members) / max(max_dist, 0.1), 1),
        })

    # Sort by density_score (companies per km), then by count
    clusters.sort(key=lambda c: (-c["density_score"], -c["count"]))
    clusters = clusters[:top_n]

    # Assign rank
    for i, cl in enumerate(clusters):
        cl["rank"] = i + 1

    return {
        "clusters": clusters,
        "total_analyzed": len(all_data),
        "total_clusters": len(clusters),
        "params": {
            "radius_km": radius_km,
            "min_companies": min_companies,
            "cell_deg": round(cell_deg, 5),
        },
    }


@app.get("/api/analyze/detail")
async def analyze_cluster_detail(
    sources: str = Query("fdic,sec,epa"),
    lat: float = Query(...),
    lon: float = Query(...),
    radius_km: float = Query(5.0),
    industry: str | None = Query(None),
):
    """Return all companies within a specific cluster area."""
    all_data = _gather_all_data(sources)
    results = []
    for item in all_data:
        if industry and item["industry"].lower() != industry.lower():
            continue
        dist = _haversine_km(lat, lon, item["lat"], item["lon"])
        if dist <= radius_km:
            results.append({**item, "_dist_km": round(dist, 3)})
    results.sort(key=lambda x: x["_dist_km"])
    return {"count": len(results), "companies": results}


app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
