"""
Automated Fresh Produce Price Tracker  (CEDA Agri Market API edition)
---------------------------------------------------------------------
1. Pull daily fruit prices + arrival quantities from the CEDA Agri Market API
   (Ashoka University, built on Agmarknet) at national and/or state level.
2. Append new rows to Google Sheets (tab: raw_prices), incrementally.
3. Rebuild analytics (tab: daily_summary) with REAL arrival-based supply signals.
4. Ask Claude (via Mesh API) for executive bullets -> tabs ai_latest / ai_history.

Usage
  python pipeline_ceda.py --probe     # no Google/LLM needed: shows what the CEDA API returns
  python pipeline_ceda.py             # full run

IMPORTANT: CEDA data is published with a lag (months, not hours) so "daily" here means
"each run picks up whatever new days CEDA has published since the last run".
Confirm query-parameter names against https://api.ceda.ashoka.edu.in/documentation/
and edit PARAMS / FIELD_ALIASES below if they differ.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests
from openai import OpenAI

try:  # only needed for the Google Sheets backend
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    gspread = Credentials = None

# ----------------------------- CONFIG ---------------------------------------
BASE = os.environ.get("CEDA_BASE_URL", "https://api.ceda.ashoka.edu.in/v1/agmarknet")
CEDA_API_KEY = os.environ.get("CEDA_API_KEY", "")
RATE_SLEEP = float(os.environ.get("CEDA_RATE_SLEEP", "6"))       # seconds between calls (API is strict)
CHUNK_DAYS = int(os.environ.get("CEDA_CHUNK_DAYS", "366"))       # date window per request (1 call/series/year)
FIRST_START = date.fromisoformat(os.environ.get("FIRST_START", "2025-01-01"))  # first-run history start
MAX_CALLS = int(os.environ.get("MAX_CALLS", "120"))              # stop politely after N API calls per run
SKIP_IF_CHECKED_DAYS = int(os.environ.get("SKIP_IF_CHECKED_DAYS", "3"))
FORCE = {"on": False}

# CEDA requires a state_id on every prices/quantities call, so we query state by state.
# Override with e.g. STATES="Madhya Pradesh,Maharashtra"
DEFAULT_STATES = "Madhya Pradesh,Maharashtra,Karnataka,Tamil Nadu,Uttar Pradesh,Gujarat"
STATES = [s.strip() for s in os.environ.get("STATES", DEFAULT_STATES).split(",") if s.strip()]
FRUITS = ["Apple", "Banana", "Mango", "Orange", "Grapes", "Papaya", "Pomegranate",
          "Pineapple", "Guava", "Water Melon", "Lemon", "Mousambi(Sweet Lime)"]

# Request-body keys. The API says "commodity id, state id, start and end date are required"
# but not the exact date key names, so we auto-detect from these candidates.
ID_KEYS = {"commodity": "commodity_id", "state": "state_id"}
DATE_KEY_VARIANTS = [("start_date", "end_date"), ("from_date", "to_date"),
                     ("startDate", "endDate"), ("start", "end"), ("from", "to")]
ACTIVE_DATE_KEYS: list = []   # filled by auto-detection
LAST_RESP: dict = {}          # last raw response per endpoint (for diagnostics)
DIAG = {"shown": False}
EXTRA_BODY = json.loads(os.environ.get("CEDA_EXTRA_BODY", "{}"))  # e.g. {"calculation_type":"daily"}
# Field-name aliases tolerated in API responses (lower-case)
FIELD_ALIASES = {
    "date": ["date", "price_date", "arrival_date", "day"],
    "min": ["min_price", "minimum_price", "min"],
    "max": ["max_price", "maximum_price", "max"],
    "modal": ["modal_price", "modal", "price_modal"],
    "qty": ["quantity", "qty", "arrival", "arrivals", "arrival_quantity", "total_quantity"],
    "c_id": ["commodity_id", "id"],
    "c_name": ["commodity_name", "name", "commodity"],
    "s_id": ["state_id", "id"],
    "s_name": ["state_name", "name", "state"],
}

MESH_API_KEY = os.environ.get("MESHAPI_API_KEY")
MESH_BASE_URL = os.environ.get("MESH_BASE_URL", "https://api.meshapi.ai/v1")
MESH_MODEL = os.environ.get("MESH_MODEL", "anthropic/claude-sonnet-4.5")

RAW_TAB, SUMMARY_TAB = "raw_prices", "daily_summary"
AI_LATEST_TAB, AI_HISTORY_TAB = "ai_latest", "ai_history"
RAW_COLS = ["record_id", "date", "geography", "commodity",
            "min_price", "max_price", "modal_price", "arrival_qty"]
NATIONAL = "All selected states"   # computed here, not an API geography

class RateLimited(Exception):
    pass


class BudgetReached(Exception):
    pass


CALLS = {"n": 0}
POST_ENDPOINTS: set = set()   # endpoints discovered to be POST-only
SESSION = requests.Session()
SESSION.headers.update({"Authorization": f"Bearer {CEDA_API_KEY}", "Accept": "application/json"})


# ----------------------------- CEDA CLIENT ----------------------------------
def call(endpoint: str, params: dict | None = None, quiet: bool = False):
    """GET (falls back to POST if the endpoint rejects GET). Returns parsed JSON or None."""
    url = f"{BASE}/{endpoint}"
    params = params or {}
    last_status = None
    n429 = 0
    for attempt in range(5):
        if CALLS["n"] >= MAX_CALLS:
            raise BudgetReached()
        CALLS["n"] += 1
        try:
            if endpoint in POST_ENDPOINTS:
                r = SESSION.post(url, json=params, timeout=60)
            else:
                r = SESSION.get(url, params=params, timeout=60)
                # Express returns 404 "Cannot GET" (or 405) when a route is POST-only
                if r.status_code == 405 or (r.status_code == 404 and "Cannot GET" in r.text):
                    r = SESSION.post(url, json=params, timeout=60)
                    POST_ENDPOINTS.add(endpoint)
            if r.status_code == 429:
                n429 += 1
                print(f"  .. {endpoint}: HTTP 429 ({n429}/3). {r.text[:100].strip()}")
                if n429 >= 3:
                    raise RateLimited()
                time.sleep([60, 120][n429 - 1])
                continue
            if r.status_code in (500, 502, 503, 504):
                wait = 2 ** attempt * 3
                ra = r.headers.get("Retry-After")
                if ra and ra.isdigit():
                    wait = max(wait, min(int(ra), 120))
                print(f"  .. {endpoint}: HTTP {r.status_code} (attempt {attempt + 1}/5), "
                      f"waiting {wait}s. {r.text[:150].strip()}")
                last_status = r.status_code
                time.sleep(wait)
                continue
            if r.status_code in (401, 403):
                raise SystemExit("CEDA auth failed (401/403): check CEDA_API_KEY / Bearer token.")
            if r.status_code >= 400:
                if not quiet:
                    print(f"  ! {endpoint} -> HTTP {r.status_code}: {r.text[:300]}  params={params}")
                return None
            time.sleep(RATE_SLEEP)
            return r.json()
        except requests.RequestException as e:
            if attempt == 4:
                print(f"  ! {endpoint}: network error {e}")
                return None
            time.sleep(2 ** attempt)
    if last_status:
        print(f"  ! {endpoint}: gave up after retries (last HTTP {last_status}). "
              f"If 429, you've hit a rate/quota limit: wait a while and raise CEDA_RATE_SLEEP.")
    return None


def walk_dicts(obj):
    """Yield every dict found anywhere inside a JSON structure."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk_dicts(v)


def extract_records(obj) -> list[dict]:
    """Find the main list of record-dicts in a response of unknown envelope shape."""
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for k in ("data", "results", "items", "prices", "quantities", "records"):
            if isinstance(obj.get(k), list):
                return [x for x in obj[k] if isinstance(x, dict)]
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found = extract_records(v)
                if found:
                    return found
    return []


def pick(rec: dict, kind: str):
    low = {str(k).lower(): v for k, v in rec.items()}
    for a in FIELD_ALIASES[kind]:
        if a in low and low[a] not in (None, ""):
            return low[a]
    return None


def cached_call(endpoint: str):
    """Catalog data rarely changes: cache it on disk so reruns don't burn API quota."""
    f = os.path.join(os.environ.get("DATA_DIR", "data"), "cache", f"{endpoint}.json")
    if os.path.exists(f):
        try:
            return json.load(open(f, encoding="utf-8"))
        except ValueError:
            pass
    payload = call(endpoint)
    if payload:
        os.makedirs(os.path.dirname(f), exist_ok=True)
        json.dump(payload, open(f, "w", encoding="utf-8"))
    return payload


def resolve_commodities() -> dict[str, int]:
    recs = extract_records(cached_call("commodities"))
    table = {}
    for r in recs:
        n, i = pick(r, "c_name"), pick(r, "c_id")
        if n is not None and i is not None:
            table[str(n).strip().lower()] = (str(n).strip(), i)
    out = {}
    for f in FRUITS:
        hit = table.get(f.lower()) or next(
            (v for k, v in table.items() if k.startswith(f.lower())), None)
        if hit:
            out[f] = hit[1]
        else:
            print(f"  ! commodity not found in CEDA catalog: {f}")
    print(f"Resolved commodities: {out}")
    return out


def _state_pairs(payload):
    """Tolerantly find (state_name, state_id) pairs in an unknown /geographies shape."""
    pairs = {}
    for d in walk_dicts(payload):
        low = {str(k).lower(): v for k, v in d.items()}
        n = next((low[k] for k in low if "state" in k and "name" in k), None)
        i = next((low[k] for k in low if "state" in k and k.endswith("id")), None)
        if n is None or i is None:   # plain {"id":..,"name":..} records
            n = n or pick(d, "s_name")
            i = i if i is not None else pick(d, "s_id")
        if isinstance(n, str) and i is not None:
            pairs.setdefault(n.strip().lower(), i)
    # dict keyed by state name, e.g. {"Maharashtra": 27, ...} or {"Maharashtra": {"id": 27}}
    if not pairs and isinstance(payload, dict):
        for k, v in payload.items():
            if isinstance(v, (int, str)) and str(v).isdigit():
                pairs[str(k).strip().lower()] = v
            elif isinstance(v, dict) and pick(v, "s_id") is not None:
                pairs[str(k).strip().lower()] = pick(v, "s_id")
    return pairs


def resolve_geographies() -> list[tuple[str, int]]:
    payload = cached_call("geographies")
    states = _state_pairs(payload)
    if not states:
        print("  ! Could not find states in /geographies. Raw response (truncated):")
        print(json.dumps(payload, indent=2, default=str)[:2500])
        return []
    geos = []
    for st in STATES:
        if st.lower() in states:
            geos.append((st, states[st.lower()]))
        else:
            print(f"  ! state not found: {st}  (available: {sorted(states)[:40]})")
    print(f"Resolved states: {geos}")
    return geos


def date_chunks(start: date, end: date, days: int):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur, nxt
        cur = nxt + timedelta(days=1)


def build_body(commodity_id, state_id, a: date, b: date, keys) -> dict:
    body = {ID_KEYS["commodity"]: commodity_id, ID_KEYS["state"]: state_id,
            keys[0]: a.isoformat(), keys[1]: b.isoformat()}
    body.update(EXTRA_BODY)
    return body


def detect_date_keys(endpoint, commodity_id, state_id, a: date, b: date):
    """Try candidate date-key names until the API stops answering 400."""
    for keys in DATE_KEY_VARIANTS:
        resp = call(endpoint, build_body(commodity_id, state_id, a, b, keys), quiet=True)
        if resp is not None:
            ACTIVE_DATE_KEYS[:] = list(keys)
            print(f"  date keys detected: {keys}")
            return resp
    # nothing worked: show the real error once
    call(endpoint, build_body(commodity_id, state_id, a, b, DATE_KEY_VARIANTS[0]))
    return None


def fetch_series(endpoint, commodity_id, state_id, start: date, end: date) -> list[dict]:
    rows = []
    for a, b in date_chunks(start, end, CHUNK_DAYS):
        if not ACTIVE_DATE_KEYS:
            resp = detect_date_keys(endpoint, commodity_id, state_id, a, b)
        else:
            resp = call(endpoint, build_body(commodity_id, state_id, a, b, ACTIVE_DATE_KEYS))
        LAST_RESP[endpoint] = resp
        rows.extend(extract_records(resp))
    return rows


def normalise(records: list[dict], kind: str) -> pd.DataFrame:
    out = []
    for r in records:
        d = pick(r, "date")
        if d is None:
            continue
        if kind == "price":
            out.append({"date": d, "min_price": pick(r, "min"), "max_price": pick(r, "max"),
                        "modal_price": pick(r, "modal")})
        else:
            out.append({"date": d, "arrival_qty": pick(r, "qty")})
    df = pd.DataFrame(out)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in df.columns.drop("date"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date"])
    agg = {c: ("sum" if c == "arrival_qty" else "mean") for c in df.columns.drop("date")}
    return df.groupby("date", as_index=False).agg(agg)


def finalize(m: pd.DataFrame) -> pd.DataFrame:
    df = m[m["modal_price"].fillna(0) > 0].copy()
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df["record_id"] = ["id_" + hashlib.md5(f"{d}|{g}|{c}".encode()).hexdigest()[:16]
                       for d, g, c in zip(df["date"], df["geography"], df["commodity"])]
    return df.drop_duplicates("record_id")[RAW_COLS].round(2)


def fetch_all(commodity_ids, geos, start_map, default_start: date, end: date, sink, state) -> int:
    """Fetch series one by one, saving progress after each. Stops cleanly on rate limit/budget."""
    total, today = 0, date.today()
    for fruit, cid in commodity_ids.items():
        for geo, sid in geos:
            key = f"{geo}|{fruit}"
            last = state.get(key)
            if last and not FORCE["on"] and (today - date.fromisoformat(last)).days < SKIP_IF_CHECKED_DAYS:
                continue
            start = start_map.get((geo, fruit), default_start)
            try:
                p = normalise(fetch_series("prices", cid, sid, start, end), "price")
                q = normalise(fetch_series("quantities", cid, sid, start, end), "qty") if not p.empty else p
            except RateLimited:
                print("  ! CEDA rate limit hit. Progress is saved; rerun later (or lower MAX_CALLS / raise CEDA_RATE_SLEEP).")
                return total
            except BudgetReached:
                print(f"  .. call budget of {MAX_CALLS} reached. Progress is saved; rerun to continue.")
                return total
            state[key] = today.isoformat()
            if p.empty:
                print(f"  {fruit} / {geo}: no price rows (nothing published for this period)")
                continue
            m = p.merge(q, on="date", how="left") if "arrival_qty" in q.columns else p.assign(arrival_qty=np.nan)
            m["geography"], m["commodity"] = geo, fruit
            df = finalize(m)
            sink(df)
            total += len(df)
            print(f"  {fruit} / {geo}: {len(df)} days ({df['date'].min()} -> {df['date'].max()})")
    return total


STATE_FILE = os.path.join(os.environ.get("DATA_DIR", "data"), "state.json")


def load_state() -> dict:
    try:
        return json.load(open(STATE_FILE, encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    json.dump(state, open(STATE_FILE, "w", encoding="utf-8"), indent=1)


# ----------------------------- PROBE ----------------------------------------
def probe():
    """Show real response shapes and find which periods actually contain data."""
    ids = resolve_commodities()
    geos = resolve_geographies()
    if not ids or not geos:
        return
    fruit, cid = next(iter(ids.items()))
    state, sid = geos[0]
    today = date.today()
    windows = [(today - timedelta(days=400), today)] + [
        (date(y, 1, 1), date(y, 12, 31)) for y in range(today.year - 1, today.year - 5, -1)]
    first_hit = None
    for a, b in windows:
        print(f"\n== /prices: {fruit} in {state}, {a} -> {b} ==")
        if ACTIVE_DATE_KEYS:
            raw = call("prices", build_body(cid, sid, a, b, ACTIVE_DATE_KEYS))
        else:
            raw = detect_date_keys("prices", cid, sid, a, b)
        recs = extract_records(raw)
        print(f"-> {len(recs)} records parsed")
        if not recs:
            print("   raw (truncated):", json.dumps(raw, default=str)[:600])
            continue
        dates = [pick(r, "date") for r in recs]
        dates = [d for d in dates if d is not None]
        print("   keys:", list(recs[0].keys()))
        print("   sample:", json.dumps(recs[0], default=str)[:400])
        if dates:
            print(f"   date range in response: {min(dates)} -> {max(dates)}")
        first_hit = first_hit or (a, b)
    if first_hit:
        a, b = first_hit
        print(f"\n== /quantities for the same window {a} -> {b} ==")
        raw = call("quantities", build_body(cid, sid, a, b, ACTIVE_DATE_KEYS or DATE_KEY_VARIANTS[0]))
        recs = extract_records(raw)
        print(f"-> {len(recs)} records; sample:", json.dumps(recs[:1], default=str)[:400])
        print("\nTo use this period: python pipeline_ceda.py --since YYYY-MM-DD --until YYYY-MM-DD")


# ----------------------------- GOOGLE SHEETS --------------------------------
def get_spreadsheet() -> gspread.Spreadsheet:
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    raw = os.environ.get("GOOGLE_CREDS_JSON")
    if raw:
        creds = Credentials.from_service_account_info(json.loads(raw), scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(
            os.environ.get("GOOGLE_CREDS_FILE", "service_account.json"), scopes=scopes)
    return gspread.authorize(creds).open_by_key(os.environ["GSHEET_ID"])


def get_ws(ss, title, header, date_col_letter=None):
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title, rows=1000, cols=max(len(header), 10))
        ws.update([header], "A1")
        if date_col_letter:  # force yyyy-mm-dd so Looker Studio sees a Date
            ws.format(f"{date_col_letter}2:{date_col_letter}",
                      {"numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}})
        return ws


def watermark(ss) -> date:
    """Start 7 days before the newest stored date (dedupe handles the overlap)."""
    try:
        ws = ss.worksheet(RAW_TAB)
        dates = pd.to_datetime(pd.Series(ws.col_values(2)[1:]), errors="coerce").dropna()
        if not dates.empty:
            return dates.max().date() - timedelta(days=7)
    except gspread.WorksheetNotFound:
        pass
    return FIRST_START


def append_new_raw(ss, df: pd.DataFrame) -> int:
    ws = get_ws(ss, RAW_TAB, RAW_COLS, "B")
    existing = set(ws.col_values(1)[1:])
    new = df[~df["record_id"].isin(existing)]
    if new.empty:
        return 0
    rows = new.astype(object).where(new.notna(), "").values.tolist()
    for i in range(0, len(rows), 5000):
        ws.append_rows(rows[i:i + 5000], value_input_option="USER_ENTERED")
    return len(new)


def read_raw(ss) -> pd.DataFrame:
    vals = ss.worksheet(RAW_TAB).get_all_values()
    df = pd.DataFrame(vals[1:], columns=vals[0])
    for c in ("min_price", "max_price", "modal_price", "arrival_qty"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["date", "modal_price"])


def overwrite_tab(ss, title, df: pd.DataFrame, date_col_letter=None):
    try:
        ws = ss.worksheet(title)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title, rows=max(len(df) + 10, 100), cols=max(len(df.columns), 10))
    out = df.replace([np.inf, -np.inf], np.nan)
    out = out.astype(object).where(out.notna(), "")
    ws.resize(rows=max(len(out) + 2, 100))
    ws.update([list(out.columns)] + out.values.tolist(), "A1", value_input_option="USER_ENTERED")
    if date_col_letter:
        ws.format(f"{date_col_letter}2:{date_col_letter}",
                  {"numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}})


# ----------------------------- CSV BACKEND (no Google API needed) ------------
DATA_DIR = os.environ.get("DATA_DIR", "data")


def _csv(name):
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, f"{name}.csv")


def csv_start_map() -> dict:
    """Per (geography, commodity) restart point, so one failed series can't leave a gap."""
    f = _csv(RAW_TAB)
    if not os.path.exists(f):
        return {}
    df = pd.read_csv(f, usecols=["date", "geography", "commodity"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    mx = df.groupby(["geography", "commodity"])["date"].max()
    return {k: v.date() - timedelta(days=7) for k, v in mx.items() if pd.notna(v)}


def csv_append_raw(df: pd.DataFrame) -> int:
    f = _csv(RAW_TAB)
    old = pd.read_csv(f) if os.path.exists(f) else pd.DataFrame(columns=RAW_COLS)
    new = df[~df["record_id"].isin(set(old["record_id"]))]
    out = pd.concat([old, new], ignore_index=True).sort_values(["date", "geography", "commodity"])
    out.to_csv(f, index=False)
    return len(new)


def csv_read_raw() -> pd.DataFrame:
    df = pd.read_csv(_csv(RAW_TAB))
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["date", "modal_price"])


def csv_write_ai(data_date, bullets, model):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    rows = pd.DataFrame({"data_as_of": data_date, "rank": range(1, len(bullets) + 1),
                         "bullet": [b.lstrip("- ").strip() for b in bullets],
                         "model": model, "generated_at": now})
    rows.to_csv(_csv(AI_LATEST_TAB), index=False)
    f = _csv(AI_HISTORY_TAB)
    hist = pd.read_csv(f) if os.path.exists(f) else rows.iloc[0:0]
    if data_date not in set(hist["data_as_of"].astype(str)):
        pd.concat([hist, rows], ignore_index=True).to_csv(f, index=False)


# ----------------------------- ANALYTICS ------------------------------------
def add_aggregate(raw: pd.DataFrame) -> pd.DataFrame:
    """Add a combined 'All selected states' series (avg price, summed arrivals)."""
    if raw["geography"].nunique() < 2:
        return raw
    agg = (raw.groupby(["date", "commodity"], as_index=False)
              .agg(min_price=("min_price", "min"), max_price=("max_price", "max"),
                   modal_price=("modal_price", "mean"), arrival_qty=("arrival_qty", "sum"),
                   n=("geography", "nunique")))
    agg = agg[agg["n"] >= max(2, raw["geography"].nunique() // 2)]  # skip thin days
    agg["geography"] = NATIONAL
    return pd.concat([raw, agg.drop(columns="n")], ignore_index=True)


def build_daily_summary(raw: pd.DataFrame) -> pd.DataFrame:
    raw = add_aggregate(raw)
    g = raw.sort_values(["geography", "commodity", "date"]).copy()
    grp = g.groupby(["geography", "commodity"])
    g["pct_change_prev"] = grp["modal_price"].pct_change() * 100
    roll = lambda s: s.rolling(7, min_periods=3).mean().shift(1)  # trailing, excludes today
    g["price_ma7"] = grp["modal_price"].transform(roll)
    g["arrivals_ma7"] = grp["arrival_qty"].transform(roll)
    g["price_index"] = g["modal_price"] / g["price_ma7"]          # >1: pricier than usual
    g["supply_index"] = g["arrival_qty"] / g["arrivals_ma7"]      # <1: fewer arrivals than usual
    g["price_spread"] = g["max_price"] - g["min_price"]
    g["signal"] = np.select(
        [(g.price_index > 1.10) & (g.supply_index < 0.85),
         g.price_index > 1.15,
         (g.price_index < 0.90) & (g.supply_index > 1.15)],
        ["Shortage risk", "Price spike", "Glut / price drop"], default="Normal")
    g["date"] = g["date"].dt.strftime("%Y-%m-%d")
    cols = ["date", "geography", "commodity", "modal_price", "min_price", "max_price",
            "price_spread", "arrival_qty", "pct_change_prev", "price_ma7", "arrivals_ma7",
            "price_index", "supply_index", "signal"]
    return g[cols].round(2)


# ----------------------------- CLAUDE SUMMARY -------------------------------
SYSTEM_PROMPT = (
    "You are a commodities analyst writing a brief for executives at a fresh-produce trading "
    "company. Use ONLY the JSON provided; never invent numbers. Prices are INR per quintal. "
    "'arrival_qty' is reported market arrivals; 'supply_index' = arrivals vs trailing average "
    "(<1 means tighter supply); 'price_index' = price vs trailing average. The data comes from "
    "a source that publishes with a delay, so refer to 'data as of <date>', never 'today'. "
    "Write 4-6 bullets, each starting with '- ' and under 30 words. Cover: top price spikes, "
    "biggest drops, likely supply shortages (signal='Shortage risk'), and one recommended action. "
    "If history is too short to compute changes, say so plainly.")


def fallback_bullets(day: pd.DataFrame) -> list[str]:
    d = day.dropna(subset=["pct_change_prev"]).sort_values("pct_change_prev")
    if d.empty:
        return [f"- {r.commodity}: modal price Rs {r.modal_price:,.0f}/qtl." for r in day.head(5).itertuples()]
    up, dn = d.iloc[-1], d.iloc[0]
    return [f"- Biggest rise: {up.commodity} {up.pct_change_prev:+.1f}% to Rs {up.modal_price:,.0f}/qtl.",
            f"- Biggest fall: {dn.commodity} {dn.pct_change_prev:+.1f}% to Rs {dn.modal_price:,.0f}/qtl."]


def generate_summary(summary: pd.DataFrame):
    base = summary[summary["geography"] == NATIONAL]
    base = base if not base.empty else summary
    latest = base["date"].max()
    day = base[base["date"] == latest]
    cols = ["commodity", "modal_price", "arrival_qty", "pct_change_prev",
            "price_index", "supply_index", "signal"]
    d = day[cols].replace({np.nan: None})
    ok = d.dropna(subset=["pct_change_prev"])
    payload = {"data_as_of": latest, "geography": base["geography"].iloc[0],
               "top_risers": ok.nlargest(5, "pct_change_prev").to_dict("records"),
               "top_fallers": ok.nsmallest(5, "pct_change_prev").to_dict("records"),
               "flagged": d[d["signal"] != "Normal"].to_dict("records"),
               "all_commodities": d.to_dict("records")}
    if not MESH_API_KEY:
        return latest, fallback_bullets(day), "rule-based"
    try:
        client = OpenAI(api_key=MESH_API_KEY, base_url=MESH_BASE_URL)
        resp = client.chat.completions.create(
            model=MESH_MODEL, temperature=0.2, max_tokens=600,
            messages=[{"role": "system", "content": SYSTEM_PROMPT},
                      {"role": "user", "content": json.dumps(payload, default=str)}])
        text = resp.choices[0].message.content.strip()
        bullets = [l.strip() for l in text.splitlines() if l.strip().startswith(("-", "•", "*"))]
        bullets = ["- " + b.lstrip("-•* ").strip() for b in bullets] or [text]
        return latest, bullets, MESH_MODEL
    except Exception as e:
        print(f"  ! LLM call failed, using rule-based summary: {e}")
        return latest, fallback_bullets(day), "rule-based (LLM error)"


def write_ai_tabs(ss, data_date, bullets, model):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    rows = pd.DataFrame({"data_as_of": data_date, "rank": range(1, len(bullets) + 1),
                         "bullet": [b.lstrip("- ").strip() for b in bullets],
                         "model": model, "generated_at": now})
    overwrite_tab(ss, AI_LATEST_TAB, rows, "A")
    hist = get_ws(ss, AI_HISTORY_TAB, list(rows.columns), "A")
    if data_date not in hist.col_values(1)[1:]:
        hist.append_rows(rows.astype(object).values.tolist(), value_input_option="USER_ENTERED")


# ----------------------------- MAIN -----------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true", help="inspect CEDA responses only")
    ap.add_argument("--since", help="override start date YYYY-MM-DD (re-fetches everything)")
    ap.add_argument("--until", help="override end date YYYY-MM-DD")
    ap.add_argument("--force", action="store_true", help="ignore the 'recently checked' skip list")
    ap.add_argument("--backend", choices=["csv", "sheets"],
                    default=os.environ.get("BACKEND") or ("sheets" if os.environ.get("GSHEET_ID") else "csv"))
    args = ap.parse_args()
    if not CEDA_API_KEY:
        raise SystemExit("Set CEDA_API_KEY (register at https://api.ceda.ashoka.edu.in).")
    if args.probe:
        try:
            return probe()
        except (RateLimited, BudgetReached):
            return print("Rate-limited or call budget reached; wait and retry.")
    use_csv = args.backend == "csv"
    print(f"Storage backend: {args.backend}  | pacing {RATE_SLEEP}s/call, max {MAX_CALLS} calls/run")

    ss = None if use_csv else get_spreadsheet()
    try:
        commodity_ids = resolve_commodities()
        geos = resolve_geographies()
    except (RateLimited, BudgetReached):
        raise SystemExit("Rate-limited while loading the catalog; wait a few minutes and rerun.")
    end = date.fromisoformat(args.until) if args.until else date.today()
    if args.since:
        FORCE["on"] = True
        start_map, default_start = {}, date.fromisoformat(args.since)
    else:
        FORCE["on"] = args.force
        start_map = csv_start_map() if use_csv else {}
        default_start = FIRST_START if use_csv else watermark(ss)
    buf: list = []
    sink = csv_append_raw if use_csv else buf.append
    state = load_state()
    print(f"Fetching up to {end} for {len(commodity_ids)} fruits x {len(geos)} states")
    try:
        total = fetch_all(commodity_ids, geos, start_map, default_start, end, sink, state)
    finally:
        save_state(state)
    if not use_csv and buf:
        append_new_raw(ss, pd.concat(buf, ignore_index=True))
    print(f"New/updated rows fetched this run: {total}  (API calls used: {CALLS['n']})")

    have = os.path.exists(_csv(RAW_TAB)) if use_csv else True
    raw = csv_read_raw() if (use_csv and have) else (None if use_csv else read_raw(ss))
    if raw is None or raw.empty:
        print("No data stored yet; rerun later or run --probe.")
        return
    summary = build_daily_summary(raw)
    if use_csv:
        summary.to_csv(_csv(SUMMARY_TAB), index=False)
    else:
        overwrite_tab(ss, SUMMARY_TAB, summary, "A")
    data_date, bullets, model = generate_summary(summary)
    if use_csv:
        csv_write_ai(data_date, bullets, model)
    else:
        write_ai_tabs(ss, data_date, bullets, model)
    print(f"AI summary (data as of {data_date}, {model}):\n" + "\n".join(bullets))


if __name__ == "__main__":
    main()