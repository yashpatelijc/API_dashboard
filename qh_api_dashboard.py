# professional_market_data_engine.py
# Integrity-first market data engine — FULL DB SHIFT (DuckDB), eager indicator precompute on Apply,
# stable star-IDs, idempotent upserts, calendar-correct contracts, and robust UI.
# Core fixes: Windows-safe Parquet export (short partitions), proper timestamp column in exports/views,
# eager indicator schema reset, and new indicators (EMA, Aroon, CCI, Price Z-Score, Return Z-Score, BB %B/BW).

import os
import json
import time
import uuid
import shutil
import hashlib
from collections import defaultdict, deque
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import requests
import streamlit as st
import plotly.graph_objects as go

from dateutil.relativedelta import relativedelta
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from multiprocessing import get_context
from streamlit_autorefresh import st_autorefresh

# =============================================================================
# 0) Page & CSS
# =============================================================================
st.set_page_config(layout="wide", page_title="Professional Market Data Engine", initial_sidebar_state="expanded")

def load_css():
    st.markdown("""
    <style>
      :root {
        --bg:#0e1117; --panel:#121826; --panel2:#141821; --ink:#eaeef7;
        --ink-dim:#bac4d6; --ink-soft:#9eb3cf; --accent:#58A6FF; --ok:#7ad97a; --warn:#ffb74d; --bad:#ef5350;
        --border:#31394d; --chip:#1f2937;
      }
      [data-testid="stAppViewContainer"] { background-color: var(--bg); color: var(--ink); }
      [data-testid="stSidebar"] { background-color: var(--panel2); color: var(--ink); }
      h1,h2,h3 { color: var(--accent); }
      .mono { font-family: ui-monospace, Menlo, Consolas, monospace;}
      .section { border: 1px solid var(--border); padding: 10px; border-radius: 12px; background: var(--panel); }
      .soft { font-size: 13px; color: var(--ink-soft); }
      .ok { color: var(--ok); font-weight:600; }
      .warn { color: var(--warn); font-weight:600; }
      .bad { color: var(--bad); font-weight:600; }
      .pill { padding: 2px 8px; border-radius: 999px; background: var(--chip); margin-left: 6px; font-size: 12px; border:1px solid #374151;}
      .kbd { font-family: ui-monospace, Menlo, monospace; padding: 1px 6px; border-radius: 6px; background: var(--chip); border: 1px solid #374151; }
      .sticky-controls { position: sticky; top: 64px; z-index: 10; background: #0e1117cc; backdrop-filter: blur(4px); padding: 8px; border-radius: 12px; border:1px solid var(--border); }
      .hint { font-size: 13px; color: var(--ink-soft); }
      .subtle { color: var(--ink-soft); font-size: 12px; }
      .tag { display:inline-block; padding:2px 8px; border:1px solid #374151; border-radius:999px; background:#1f2937; color:#cbd5e1; font-size:12px; margin:2px 4px; }
    </style>
    """, unsafe_allow_html=True)

load_css()
st.title("Professional Market Data Engine (Trusted) — DB Edition")

def _st_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

APP_VERSION = "2.2-db-hardened+eager-ind+schema-reset+short-parquet+new-indicators"

# =============================================================================
# 1) Constants & Defaults
# =============================================================================
ALL_PRODUCT_CODES = ["SRA", "FF", "ER", "S1R", "SON", "FSR", "FER", "FERER", "S1RFF", "JTOA", "YBA", "NBB", "CRA"]
ALL_INTERMARKET_PAIRS = [
    ("SRA","FSR"), ("SRA","ER"), ("SRA","SON"), ("SRA","FER"), ("SRA","JTOA"), ("SRA","YBA"), ("SRA","NBB"), ("SRA","CRA"),
    ("FF","S1R"),
    ("ER","SON"), ("ER","FER"), ("ER","JTOA"), ("ER","YBA"), ("ER","NBB"), ("ER","CRA"),
    ("SON","FER"), ("SON","JTOA"), ("SON","YBA"), ("SON","NBB"), ("SON","CRA"),
    ("FER","JTOA"), ("FER","YBA"), ("FER","NBB"), ("FER","CRA"),
    ("JTOA","YBA"), ("JTOA","NBB"), ("JTOA","CRA"),
    ("YBA","NBB"), ("YBA","CRA"),
    ("NBB","CRA"),
]

DEFAULT_ABS_RECON_THR = 1e12
DEFAULT_REL_RECON_THR = 9.99
DEFAULT_FRESH_MINUTES = 3
DEFAULT_PAIR_TOLERANCE_SECONDS   = 86400
DEFAULT_SPAN_TOLERANCE_SECONDS   = 86400
DEFAULT_MAX_LEG_AGE_SECONDS      = 86400
DEFAULT_STALE_1M_SECONDS         = 72 * 3600

MAX_REQUESTS_PER_MINUTE = 50
MAX_INSTRUMENTS_PER_REQUEST = 50

DEFAULT_TOKEN = "your_token_here"

# Indicator families (defaults)
DEFAULT_INDICATOR_SETS = {
    "TR": True,
    "EMA": [20, 50, 100],          # NEW
    "ATR": [10, 14, 20],
    "RSI": [14],
    "ADX": [14],
    "SMA": [5, 10, 20, 30, 50, 100],
    "Ranges": [5, 10, 20],
    "Range/ATR": {"ranges": [20], "atrs": [20]},
    "Range/Range": {"numerators": [20], "denominators": [10]},
    "BB": {"periods": [20], "stds": [2.0]},  # %B & Bandwidth now derived here too
    "KC": {"periods": [20], "multipliers": [2.0], "atr_periods": [20]},
    "SuperTrend": {"atr_periods": [10], "multipliers": [2.0]},
    "MACD": {"fast": [12], "slow": [26], "signal": [9]},
    "OBV": True,
    "Aroon": [14],                 # NEW
    "CCI": [20],                   # NEW
    "ZPRICE": [20],                # NEW price z-score windows
    "ZRET": [20],                  # NEW return z-score windows
}
INDICATOR_FAMILY_MENU = [
    "TR","EMA","ATR","RSI","ADX","SMA","Ranges","Range/ATR","Range/Range","BB","KC","SuperTrend","MACD","OBV",
    "Aroon","CCI","ZPRICE","ZRET"
]
INDICATOR_FAMILY_ALL = ["ALL"] + INDICATOR_FAMILY_MENU

# =============================================================================
# 2) Session State
# =============================================================================
for k, v in {
    'prices_1m': {},
    'series': {'1D': {}, '1H': {}, '5M': {}},
    'syn_1m': {},
    'syn_series': {'1D': {}, '1H': {}, '5M': {}},
    'withheld': defaultdict(lambda: defaultdict(list)),
    'audit_scale': [],
    'last_update': "N/A",
    'auth_error': False,
    'progress': {'fetch': {}, 'ind': {}},
    'trusted_api_1m': set(),
    'trusted_syn_1m': set(),
    'rate_calls': deque(),
    'selection_signatures': {},
    'loaded_contracts': {'markets': {}, 'intermarket': {}},
    'symbol_meta': {},
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

if 'access_token' not in st.session_state:
    st.session_state.access_token = DEFAULT_TOKEN
if 'auth_scheme' not in st.session_state:
    st.session_state.auth_scheme = "Bearer"
if 'store_root' not in st.session_state:
    st.session_state.store_root = os.path.join(os.getcwd(), "data_engine")
if 'cfg_applied' not in st.session_state:
    st.session_state.cfg_applied = False
if 'cfg' not in st.session_state:
    st.session_state.cfg = {}
if 'last_snapshot_ts' not in st.session_state:
    st.session_state.last_snapshot_ts = 0.0

# =============================================================================
# 3) Utilities: time, hashing, months, rate-limit, dirs
# =============================================================================
def utc_ms_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def stable_u63(s: str) -> int:
    """Deterministic, collision-resistant 63-bit integer ID."""
    d = hashlib.blake2b(s.encode('utf-8'), digest_size=8).digest()
    v = int.from_bytes(d, 'big', signed=False)
    return v & 0x7FFFFFFFFFFFFFFF  # fit into signed BIGINT

class RateLimiter:
    def __init__(self, max_per_minute: int):
        self.max = max_per_minute
        self.calls = deque()
    def wait(self):
        now = time.time()
        while self.calls and now - self.calls[-1] > 60.0:
            self.calls.pop()
        if len(self.calls) >= self.max:
            sleep_for = 60 - (now - self.calls[-1]) + 0.05
            time.sleep(max(0, sleep_for))
        self.calls.appendleft(time.time())

if 'rate_limiter' not in st.session_state:
    st.session_state.rate_limiter = RateLimiter(MAX_REQUESTS_PER_MINUTE)

def rate_limited_get(url: str, headers: dict, timeout: int):
    st.session_state.rate_limiter.wait()
    return requests.get(url, headers=headers, timeout=timeout)

def get_month_codes(): return {1:'F', 2:'G', 3:'H', 4:'J', 5:'K', 6:'M', 7:'N', 8:'Q', 9:'U', 10:'V', 11:'X', 12:'Z'}
INV_MONTH = {v:k for k,v in get_month_codes().items()}

def parse_tail_to_ym(tail: str):
    try:
        c = tail[0]; yy = int(tail[1:])
        y = 2000 + yy; m = INV_MONTH.get(c)
        return (y, m if m else 99) if m else (9999, 99)
    except Exception:
        return (9999, 99)

def extract_tails(symbol: str):
    parts = symbol.split("-")
    if len(parts) == 1:
        last3 = symbol[-3:]
        return [last3] if (len(last3) == 3 and last3[0].isalpha() and last3[1:].isdigit()) else []
    tails = []
    for seg in parts[1:]:
        s = seg[-3:]
        if len(s) == 3 and s[0].isalpha() and s[1:].isdigit():
            tails.append(s)
    if not tails and len(parts[0]) >= 3:
        s = parts[0][-3:]
        if len(s) == 3 and s[0].isalpha() and s[1:].isdigit():
            tails.append(s)
    return tails

def calendar_key(symbol: str):
    tails = extract_tails(symbol)
    first = tails[0] if tails else symbol[-3:]
    return parse_tail_to_ym(first)

def sorted_calendar(seq):
    try:
        return sorted(seq, key=calendar_key)
    except Exception:
        return sorted(list(seq))

def months_ahead_from_ym(y:int, m:int) -> int:
    today = date.today()
    return (y - today.year) * 12 + (m - today.month)

def curve_bucket_for_symbol(symbol: str) -> str | None:
    tails = extract_tails(symbol)
    if not tails:
        return None
    yms = [parse_tail_to_ym(t) for t in tails]
    y, m = min(yms)
    if y == 9999 or m == 99:
        return None
    ahead = max(0, months_ahead_from_ym(y, m))
    if ahead <= 12: return "front"
    if ahead <= 30: return "mid"
    return "back"

def human_age(ms):
    if ms is None: return "N/A"
    secs = int((utc_ms_now() - int(ms))/1000)
    secs = max(0, secs)
    d, r = divmod(secs, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    if d: return f"{d}d {h}h"
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"

# =============================================================================
# 4) API Fetch
# =============================================================================
@st.cache_data(ttl=240)
def fetch_latest_1m_batch(instruments_str, auth_header_value):
    base_url = "https://qh-api.corp.hertshtengroup.com/api/v2/ohlc/"
    url = f"{base_url}?instruments={instruments_str}&interval=1M&count=1"
    headers = {"Authorization": auth_header_value, "Accept": "application/json"}
    try:
        resp = rate_limited_get(url, headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        out = {}
        if isinstance(data, list):
            for d in data:
                if not isinstance(d, dict): continue
                p = d.get("product"); c = d.get("close"); t = d.get("time")
                if p is None or c is None or t is None: continue
                if p not in out or int(t) > out[p]['time']:
                    out[p] = {'close': float(c), 'time': int(t)}
        return out
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401: return None
        return {}
    except requests.RequestException:
        return {}

@st.cache_data(ttl=240)
def fetch_series_batch(instruments_str, auth_header_value, interval, count, timeout_sec):
    base_url = "https://qh-api.corp.hertshtengroup.com/api/v2/ohlc/"
    url = f"{base_url}?instruments={instruments_str}&interval={interval}&count={count}"
    headers = {"Authorization": auth_header_value, "Accept": "application/json"}
    try:
        resp = rate_limited_get(url, headers, timeout=timeout_sec)
        resp.raise_for_status()
        data = resp.json()
        result = {}
        if isinstance(data, list) and data and isinstance(data[0], dict) and 'candles' in data[0]:
            for item in data:
                product = item.get('product'); candles = item.get('candles', [])
                cleaned = []
                for c in candles:
                    if isinstance(c, dict) and all(k in c for k in ('time','open','high','low','close')):
                        cleaned.append({
                            'time': int(c['time']),
                            'open': float(c['open']),
                            'high': float(c['high']),
                            'low':  float(c['low']),
                            'close':float(c['close']),
                            'volume': float(c.get('volume', 0.0))
                        })
                if product and cleaned:
                    cleaned.sort(key=lambda x: x['time'])
                    result[product] = cleaned[-count:]
        elif isinstance(data, list):
            buckets = {}
            for row in data:
                if not isinstance(row, dict): continue
                product = row.get('product')
                if product is None or not all(k in row for k in ('time','open','high','low','close')): continue
                bar = {
                    'time': int(row['time']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low':  float(row['low']),
                    'close':float(row['close']),
                    'volume': float(row.get('volume', 0.0))
                }
                buckets.setdefault(product, []).append(bar)
            for prod, bars in buckets.items():
                bars.sort(key=lambda x: x['time'])
                result[prod] = bars[-count:]
        return result
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401: return None
        return {}
    except requests.RequestException:
        return {}

def build_auth_header_value(token: str, scheme: str) -> str:
    t = (token or "").strip()
    s = (scheme or "Bearer").strip()
    if s.lower().startswith("raw"):
        return t
    if t.lower().startswith("bearer ") or t.lower().startswith("jwt "):
        return t
    return f"{s} {t}"

# =============================================================================
# 5) Validation
# =============================================================================
def validate_api_latest_1m_map(m: dict, withheld: dict):
    ok = {}
    for sym, rec in m.items():
        try:
            t = rec.get('time'); c = rec.get('close')
            if t is None or c is None:
                withheld[sym]['1M'].append("BAD_SCHEMA"); continue
            c = float(c); t = int(t)
            if not np.isfinite(c):
                withheld[sym]['1M'].append("BAD_VALUE"); continue
            ok[sym] = {'close': c, 'time': t}
        except Exception:
            withheld[sym]['1M'].append("BAD_SCHEMA")
    return ok

def series_monotonic_and_ohlc_ok(bars):
    prev_t = None
    for b in bars:
        t = b.get('time'); o=b.get('open'); h=b.get('high'); l=b.get('low'); c=b.get('close')
        if not all(k is not None for k in (t,o,h,l,c)): return False
        if not all(np.isfinite(float(x)) for x in (o,h,l,c)): return False
        if prev_t is not None and int(t) <= int(prev_t): return False
        if float(h) < max(float(o), float(c)): return False
        if float(l) > min(float(o), float(c)): return False
        if float(h) < float(l): return False
        prev_t = t
    return True

def validate_api_series_dict(d: dict, interval: str, withheld: dict):
    ok = {}
    for sym, bars in d.items():
        try:
            if not isinstance(bars, list) or not bars:
                withheld[sym][interval].append("EMPTY_SERIES"); continue
            bars = sorted(bars, key=lambda x: x['time'])
            dedup = []
            seen = set()
            for b in bars:
                t = b.get('time')
                if t in seen: continue
                seen.add(t); dedup.append(b)
            if not series_monotonic_and_ohlc_ok(dedup):
                withheld[sym][interval].append("OHLC_INVARIANT"); continue
            ok[sym] = dedup
        except Exception:
            withheld[sym][interval].append("BAD_SCHEMA")
    return ok

# =============================================================================
# 6) Universe & Intermarket
# =============================================================================
def _ym_from_tail(tail: str): return parse_tail_to_ym(tail)
def _is_asc_pair(t1: str, t2: str) -> bool:
    y1, m1 = _ym_from_tail(t1); y2, m2 = _ym_from_tail(t2)
    return (y2, m2) > (y1, m1)
def _is_asc_triplet(t1: str, t2: str, t3: str) -> bool:
    y1, m1 = _ym_from_tail(t1); y2, m2 = _ym_from_tail(t2); y3, m3 = _ym_from_tail(t3)
    return (y2, m2) > (y1, m1) and (y3, m3) > (y2, m2)

def _filter_bad_spreads_generic(spreads_dict: dict, inter: bool):
    out = {}
    for t, lst in spreads_dict.items():
        keep = []
        for s in lst:
            parts = s.split("-")
            if inter:
                if len(parts) == 3:
                    left = parts[1][-3:]; right = parts[2]
                    if _is_asc_pair(left, right): keep.append(s)
            else:
                if len(parts) == 2:
                    left = parts[0][-3:]; right = parts[1]
                    if _is_asc_pair(left, right): keep.append(s)
        out[t] = keep
    return out

def _filter_bad_flies_generic(flies_dict: dict, inter: bool):
    out = {}
    for t, lst in flies_dict.items():
        keep = []
        for f in lst:
            parts = f.split("-")
            if inter:
                if len(parts) == 4:
                    t1 = parts[1][-3:]; t2 = parts[2]; t3 = parts[3]
                    if _is_asc_triplet(t1, t2, t3): keep.append(f)
            else:
                if len(parts) == 3:
                    t1 = parts[0][-3:]; t2 = parts[1]; t3 = parts[2]
                    if _is_asc_triplet(t1, t2, t3): keep.append(f)
        out[t] = keep
    return out

def _filter_bad_spreads(spreads_dict: dict): return _filter_bad_spreads_generic(spreads_dict, inter=False)
def _filter_bad_flies(flies_dict: dict):     return _filter_bad_flies_generic(flies_dict, inter=False)

@st.cache_data
def generate_intra(product_code, num_consecutive, num_quarterly, tenors):
    month_codes = get_month_codes()
    start_date = date.today()
    seen = set()
    details = []
    last_date = start_date - relativedelta(days=1)
    for i in range(num_consecutive):
        d = start_date + relativedelta(months=i)
        if (d.year, d.month) not in seen:
            details.append((d, month_codes[d.month], str(d.year)[-2:]))
            seen.add((d.year, d.month)); last_date = d
    q_months, yrs_added = {3,6,9,12}, set()
    qd = last_date; guard = 0
    while len(yrs_added) < num_quarterly and guard < 600:
        qd += relativedelta(months=1); guard += 1
        if qd.month in q_months:
            yrs_added.add(qd.year)
            if (qd.year, qd.month) not in seen:
                details.append((qd, month_codes[qd.month], str(qd.year)[-2:]))
                seen.add((qd.year, qd.month))
    details.sort()
    outrights = [f"{product_code}{c}{yy}" for _,c,yy in details]
    spreads = {t: [] for t in tenors}
    for i in range(len(details)):
        for j in range(i+1, len(details)):
            d1,c1,y1 = details[i]; d2,c2,y2 = details[j]
            diff = (d2.year-d1.year)*12 + (d2.month-d1.month)
            if diff in spreads:
                spreads[diff].append(f"{product_code}{c1}{y1}-{c2}{y2}")
    flies = {t: [] for t in tenors}
    for i in range(len(details)):
        for j in range(i+1, len(details)):
            for k in range(j+1, len(details)):
                d1,c1,y1 = details[i]; d2,c2,y2 = details[j]; d3,c3,y3 = details[k]
                diff1 = (d2.year-d1.year)*12 + (d2.month-d1.month)
                diff2 = (d3.year-d2.year)*12 + (d3.month-d2.month)
                if diff1 == diff2 and diff1 in flies:
                    flies[diff1].append(f"{product_code}{c1}{y1}-{c2}{y2}-{c3}{y3}")
    spreads = _filter_bad_spreads(spreads)
    flies   = _filter_bad_flies(flies)
    outrights = sorted_calendar(outrights)
    for t in spreads: spreads[t] = sorted_calendar(spreads[t])
    for t in flies:   flies[t]   = sorted_calendar(flies[t])
    return outrights, spreads, flies

def details_from_outrights(outright_list):
    out = []
    for s in outright_list:
        tail = s[-3:]
        y,m = parse_tail_to_ym(tail)
        c = tail[0]; yy = tail[1:]
        if y != 9999 and m != 99:
            out.append((y,m,c,yy))
    out.sort()
    return out

def generate_inter(contracts_by_product, tenors, enabled_pairs):
    defs = {}
    for (a,b) in enabled_pairs:
        if a not in contracts_by_product or b not in contracts_by_product: continue
        a_out = contracts_by_product[a]['outr']; b_out = contracts_by_product[b]['outr']
        da = details_from_outrights(a_out); db = details_from_outrights(b_out)
        set_a = {(y,m,c,yy) for (y,m,c,yy) in da}
        set_b = {(y,m,c,yy) for (y,m,c,yy) in db}
        common = sorted(list(set_a & set_b))
        outr = []; spr = {t: [] for t in tenors}; fly = {t: [] for t in tenors}
        legs_out = {}; legs_spr = {}; legs_fly = {}
        for (y,m,c,yy) in common:
            io = f"{a}-{b}{c}{yy}"; outr.append(io); legs_out[io] = [f"{a}{c}{yy}", f"{b}{c}{yy}"]
        for i in range(len(common)):
            for j in range(i+1, len(common)):
                y1,m1,c1,yy1 = common[i]; y2,m2,c2,yy2 = common[j]
                diff = (y2-y1)*12 + (m2-m1)
                if diff in spr:
                    s = f"{a}-{b}{c1}{yy1}-{c2}{yy2}"
                    spr[diff].append(s); legs_spr[s] = [f"{a}-{b}{c1}{yy1}", f"{a}-{b}{c2}{yy2}"]
        for i in range(len(common)):
            for j in range(i+1, len(common)):
                for k in range(j+1, len(common)):
                    y1,m1,c1,yy1 = common[i]; y2,m2,c2,yy2 = common[j]; y3,m3,c3,yy3 = common[k]
                    diff1 = (y2-y1)*12 + (m2-m1); diff2 = (y3-y2)*12 + (m3-m2)
                    if diff1 == diff2 and diff1 in fly:
                        f = f"{a}-{b}{c1}{yy1}-{c2}{yy2}-{c3}{yy3}"
                        fly[diff1].append(f); legs_fly[f] = [f"{a}-{b}{c1}{yy1}", f"{a}-{b}{c2}{yy2}", f"{a}-{b}{c3}{yy3}"]
        outr = sorted_calendar(outr)
        for t in spr: spr[t] = sorted_calendar(spr[t])
        for t in fly: fly[t] = sorted_calendar(fly[t])
        defs[(a,b)] = {'outr': outr, 'spr': spr, 'fly': fly, 'legs_out': legs_out, 'legs_spr': legs_spr, 'legs_fly': legs_fly}
    def _clean_inter_defs(defs_in):
        for _, dd in defs_in.items():
            dd['spr'] = _filter_bad_spreads_generic(dd['spr'], inter=True)
            dd['fly'] = _filter_bad_flies_generic(dd['fly'], inter=True)
        return defs_in
    return _clean_inter_defs(defs)

# =============================================================================
# 7) Synthetic
# =============================================================================
def age_seconds(ts_ms):
    if not isinstance(ts_ms, (int,float)): return float('inf')
    return (utc_ms_now() - ts_ms)/1000.0

def within_pair_tolerance(ts_list, pair_tol_sec: int) -> bool:
    for i in range(len(ts_list)):
        for j in range(i+1, len(ts_list)):
            if not (isinstance(ts_list[i], (int,float)) and isinstance(ts_list[j], (int,float))): return False
            if abs(ts_list[i]-ts_list[j])/1000.0 > pair_tol_sec: return False
    return True

def within_span_tolerance(ts_list, span_tol_sec: int) -> bool:
    if not ts_list or any(not isinstance(t, (int,float)) for t in ts_list): return False
    return (max(ts_list)-min(ts_list))/1000.0 <= span_tol_sec

def legs_fresh_enough(recs, max_leg_age_sec: int) -> bool:
    return all(age_seconds(r.get("time")) <= max_leg_age_sec for r in recs)

def weighted_latest(leg_recs, weights, pair_tol_sec, span_tol_sec, max_leg_age_sec):
    ts = [r.get("time") for r in leg_recs]
    if not (within_pair_tolerance(ts, pair_tol_sec) and within_span_tolerance(ts, span_tol_sec)): return None
    if not legs_fresh_enough(leg_recs, max_leg_age_sec): return None
    try:
        val = sum(float(r['close'])*w for r,w in zip(leg_recs, weights))
    except Exception:
        return None
    return {"close": float(val), "time": int(min(ts))}

def weighted_series(leg_series_list, weights, cap):
    if not leg_series_list: return []
    try:
        time_sets = [set(int(b['time']) for b in s) for s in leg_series_list]
        if not time_sets: return []
        common_all = sorted(set.intersection(*time_sets))
        target_len = min(*[len(s) for s in leg_series_list], cap)
        common_ts = [int(t) for t in common_all[-target_len:]]
        idx_maps = [{int(b['time']): b for b in s} for s in leg_series_list]
        out = []
        for t in common_ts:
            try:
                c = sum(float(im[t]['close'])*w for im,w in zip(idx_maps, weights))
            except Exception:
                continue
            out.append({"time": t, "open": c, "high": c, "low": c, "close": c, "volume": 0.0})
        return out
    except Exception:
        return []

# =============================================================================
# 8) Reconciliation & Strategy Type
# =============================================================================
def power_of_10_candidates(): return [10.0 ** k for k in range(-6, 7)]

def robust_mape(a, b):
    a = np.asarray(a, dtype=float); b = np.asarray(b, dtype=float)
    mask = np.isfinite(a) & np.isfinite(b) & (np.abs(b) > 0)
    if not mask.any(): return np.inf
    return float(np.mean(np.abs((a[mask] - b[mask]) / np.abs(b[mask]))))

def choose_scale_factor(api_vals, syn_vals, tol=0.5, min_overlap=5):
    a = np.asarray(api_vals, dtype=float); s = np.asarray(syn_vals, dtype=float)
    mask = np.isfinite(a) & np.isfinite(s) & (np.abs(s) > 0)
    a = a[mask]; s = s[mask]
    if len(a) < min_overlap:
        return (1.0, np.inf, len(a), False, "INSUFFICIENT_OVERLAP")
    best = (np.inf, 1.0)
    for f in power_of_10_candidates():
        m = robust_mape(a * f, s)
        if m < best[0]: best = (m, f)
    mape, factor = best
    accepted = mape <= tol
    return (factor, mape, len(a), accepted, "OK" if accepted else "MAPE_EXCEEDS_TOL")

def reconcile_series(api_bars, syn_bars, abs_thr, rel_thr, factor=1.0):
    if not api_bars and not syn_bars: return (False, False)
    if api_bars and not syn_bars: return (True, False)
    if syn_bars and not api_bars: return (False, True)
    da = {int(b['time']): float(b['close']) for b in api_bars}
    ds = {int(b['time']): float(b['close']) for b in syn_bars}
    overlap = sorted(set(da.keys()) & set(ds.keys()))
    if not overlap: return (True, True)
    for t in overlap:
        a = da[t] * factor; s = ds[t]
        thr = max(abs_thr, rel_thr * abs(s))
        if abs(a - s) > thr: return (False, True)
    return (True, True)

def reconcile_latest_1m(api_rec, syn_rec, abs_thr, rel_thr, factor=1.0):
    if (api_rec is None) and (syn_rec is None): return (False, False)
    if (api_rec is not None) and (syn_rec is None): return (True, False)
    if (api_rec is None) and (syn_rec is not None): return (False, True)
    a = float(api_rec['close']) * factor; s = float(syn_rec['close'])
    thr = max(abs_thr, rel_thr * abs(s))
    return (abs(a - s) <= thr, True)

def strategy_type_for(symbol: str):
    parts = symbol.split("-")
    if len(parts) == 1:
        return "outright"
    elif len(parts) == 2:
        left, right = parts
        if len(left) > 3 and len(right) == 3:
            return "spread"
        if len(left) <= 3 and len(right) == 3:
            return "inter_outright"
        return "spread"
    elif len(parts) == 3:
        return "inter_spread" if len(parts[0]) <= 3 else "fly"
    elif len(parts) == 4:
        return "inter_fly"
    return "structure"

# =============================================================================
# 9) Indicators — compute
# =============================================================================
LEGACY_VALUE_NAMES = {
    "ATR10": ("ATR", 10), "ATR20": ("ATR", 20),
    "High_5": ("Ranges", 5), "Low_5": ("Ranges", 5), "Range_5": ("Ranges", 5),
    "High_10": ("Ranges", 10), "Low_10": ("Ranges", 10), "Range_10": ("Ranges", 10),
    "High_20": ("Ranges", 20), "Low_20": ("Ranges", 20), "Range_20": ("Ranges", 20),
    "Range_20_over_ATR20": ("Range/ATR", (20, 20)),
    "RSI14": ("RSI", 14),
    "MACD_line": ("MACD", None), "MACD_signal": ("MACD", None), "MACD_hist": ("MACD", None),
    "DMI_plus": ("ADX", 14), "DMI_minus": ("ADX", 14), "ADX14": ("ADX", 14),
    "SMA_5": ("SMA", 5), "SMA_10": ("SMA", 10), "SMA_20": ("SMA", 20), "SMA_30": ("SMA", 30), "SMA_50": ("SMA", 50), "SMA_100": ("SMA", 100),
    "BB_MID": ("BB", (20, 2.0)), "BB_UP_2x": ("BB", (20, 2.0)), "BB_DN_2x": ("BB", (20, 2.0)),
    "KC_MID": ("KC", (20, 2.0, 20)), "KC_UP_2x": ("KC", (20, 2.0, 20)), "KC_DN_2x": ("KC", (20, 2.0, 20)),
    "SuperTrend_UB_2x": ("SuperTrend", (10, 2.0)), "SuperTrend_LB_2x": ("SuperTrend", (10, 2.0)), "OBV": ("OBV", None),
}

def _normalize_family_selection(sel_list):
    sel = set(sel_list or [])
    if "ALL" in sel: return set(INDICATOR_FAMILY_MENU), {}
    families = set([x for x in sel if x in INDICATOR_FAMILY_MENU or x == "ALL"])
    legacy_forced = {}
    for x in sel:
        if x in LEGACY_VALUE_NAMES:
            fam, val = LEGACY_VALUE_NAMES[x]
            families.add(fam)
            legacy_forced.setdefault(fam, set()).add(val)
    return families, legacy_forced

def _resolve_indicator_sets(cfg_sets, legacy_forced):
    s = json.loads(json.dumps(cfg_sets))
    for fam in ["ATR","RSI","ADX","SMA","Ranges"]:
        if fam in legacy_forced:
            s[fam] = sorted(set([int(v) for v in (s.get(fam, []) or [])] + [int(v) for v in legacy_forced[fam] if isinstance(v, (int, float))]))
    return s

def compute_individual_indicators(series_list, families_selected: set, cfg_sets, scope: str):
    if not series_list: return pd.DataFrame()
    df = pd.DataFrame(series_list).sort_values('time').drop_duplicates('time')
    need_cols = {'time','open','high','low','close'}
    if not need_cols.issubset(df.columns): return pd.DataFrame()
    # keep Date in memory only; it will be dropped before DB upsert
    df['Date'] = pd.to_datetime(df['time'], unit='ms')
    for c in ['open','high','low','close','volume']:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce')
    close = df['close']; high = df['high']; low = df['low']; volume = df.get('volume', pd.Series(0.0, index=df.index))

    # TR
    if any(f in families_selected for f in ("TR","ATR","ADX","KC","SuperTrend")):
        prev_close = close.shift(1)
        tr1 = (high - low).abs()
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # EMA (generic overlays) — NEW
    if "EMA" in families_selected:
        for n in sorted(set(int(x) for x in (cfg_sets.get("EMA", []) or []))):
            df[f'EMA_{n}'] = close.shift(1).ewm(span=max(1,n), adjust=False).mean()

    # ATR
    if "ATR" in families_selected:
        for n in sorted(set(int(x) for x in (cfg_sets.get("ATR", []) or []))):
            df[f'ATR{n}'] = df['TR'].shift(1).ewm(alpha=1/max(n,1), adjust=False).mean()

    # RSI
    if "RSI" in families_selected:
        delta = close.diff()
        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        for n in sorted(set(int(x) for x in (cfg_sets.get("RSI", []) or []))):
            avg_gain = gain.shift(1).ewm(alpha=1/max(n,1), adjust=False).mean()
            avg_loss = loss.shift(1).ewm(alpha=1/max(n,1), adjust=False).mean()
            rs = avg_gain / avg_loss.replace(0, np.nan)
            rsi = 100 - (100 / (1 + rs))
            rsi[avg_loss == 0] = 100.0
            df[f'RSI_{n}'] = rsi

    # ADX
    if "ADX" in families_selected:
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low
        dm_plus = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        dm_minus = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        dm_plus = pd.Series(dm_plus, index=df.index)
        dm_minus = pd.Series(dm_minus, index=df.index)
        for n in sorted(set(int(x) for x in (cfg_sets.get("ADX", []) or []))):
            tr_sm = df['TR'].shift(1).ewm(alpha=1/max(n,1), adjust=False).mean()
            dmp_sm = dm_plus.shift(1).ewm(alpha=1/max(n,1), adjust=False).mean()
            dmm_sm = dm_minus.shift(1).ewm(alpha=1/max(n,1), adjust=False).mean()
            with np.errstate(divide='ignore', invalid='ignore'):
                di_plus = 100 * (dmp_sm / tr_sm.replace(0, np.nan))
                di_minus = 100 * (dmm_sm / tr_sm.replace(0, np.nan))
                dx = 100 * ((di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan))
            df[f'DIplus_{n}'] = di_plus
            df[f'DIminus_{n}'] = di_minus
            df[f'ADX_{n}'] = dx.shift(1).ewm(alpha=1/max(n,1), adjust=False).mean()

    # Ranges
    needed_ranges = set()
    if "Ranges" in families_selected:
        needed_ranges.update(int(x) for x in (cfg_sets.get("Ranges", []) or []))
    if "Range/ATR" in families_selected:
        rr = cfg_sets.get("Range/ATR", {"ranges":[20]})
        needed_ranges.update(int(x) for x in rr.get("ranges", []) or [])
    if "Range/Range" in families_selected:
        r2 = cfg_sets.get("Range/Range", {"numerators":[20], "denominators":[10]})
        needed_ranges.update(int(x) for x in (r2.get("numerators", []) or []))
        needed_ranges.update(int(x) for x in (r2.get("denominators", []) or []))
    if needed_ranges:
        for n in sorted(set(needed_ranges)):
            hh = high.shift(1).rolling(n, min_periods=n).max()
            ll = low.shift(1).rolling(n, min_periods=n).min()
            df[f'High_{n}'] = hh
            df[f'Low_{n}'] = ll
            df[f'Range_{n}'] = hh - ll

    # Range/ATR
    if "Range/ATR" in families_selected:
        r_cfg = cfg_sets.get("Range/ATR", {"ranges":[20], "atrs":[20]})
        ranges = sorted(set(int(x) for x in r_cfg.get("ranges", []) or []))
        atrs   = sorted(set(int(x) for x in r_cfg.get("atrs", []) or []))
        for a in atrs:
            col = f'ATR{a}'
            if col not in df.columns and "TR" in df.columns:
                df[col] = df['TR'].shift(1).ewm(alpha=1/max(a,1), adjust=False).mean()
        for r in ranges:
            for a in atrs:
                rcol = f'Range_{r}'; acol = f'ATR{a}'
                if (rcol in df.columns) and (acol in df.columns):
                    df[f'Range_{r}_over_ATR{a}'] = df[rcol] / df[acol].replace(0, np.nan)

    # Range/Range
    if "Range/Range" in families_selected:
        r2 = cfg_sets.get("Range/Range", {"numerators":[20], "denominators":[10]})
        nums = sorted(set(int(x) for x in r2.get("numerators", []) or []))
        dens = sorted(set(int(x) for x in r2.get("denominators", []) or []))
        for m in nums:
            for n in dens:
                if m > n and (f'Range_{m}' in df.columns) and (f'Range_{n}' in df.columns):
                    df[f'Range_{m}_over_Range_{n}'] = df[f'Range_{m}'] / df[f'Range_{n}'].replace(0, np.nan)

    # BB + %B + Bandwidth — ENHANCED
    if "BB" in families_selected:
        bb = cfg_sets.get("BB", {"periods":[20], "stds":[2.0]})
        periods = sorted(set(int(x) for x in bb.get("periods", []) or []))
        stds    = sorted(set(float(x) for x in bb.get("stds", []) or []))
        for p in periods:
            mid = close.shift(1).rolling(p, min_periods=p).mean()
            std = close.shift(1).rolling(p, min_periods=p).std(ddof=0)
            df[f'BB_MID_{p}'] = mid
            for k in stds:
                up = mid + float(k) * std
                dn = mid - float(k) * std
                df[f'BB_UP_{p}_{k}x'] = up
                df[f'BB_DN_{p}_{k}x'] = dn
                rng = (up - dn)
                df[f'BB_pctB_{p}_{k}x'] = (close - dn) / rng.replace(0, np.nan)
                df[f'BB_BW_{p}_{k}x'] = rng / mid.replace(0, np.nan)

    # KC
    if "KC" in families_selected:
        kc = cfg_sets.get("KC", {"periods":[20], "multipliers":[2.0], "atr_periods":[20]})
        periods = sorted(set(int(x) for x in kc.get("periods", []) or []))
        mults   = sorted(set(float(x) for x in kc.get("multipliers", []) or []))
        apers   = sorted(set(int(x) for x in kc.get("atr_periods", []) or []))
        for p in periods:
            mid = close.shift(1).ewm(span=p, adjust=False).mean()
            df[f'KC_MID_{p}'] = mid
            for a in apers:
                atrcol = f'ATR{a}'
                if atrcol not in df.columns and "TR" in df.columns:
                    df[atrcol] = df['TR'].shift(1).ewm(alpha=1/max(a,1), adjust=False).mean()
                for m in mults:
                    df[f'KC_UP_{p}_{m}x_ATR{a}'] = mid + float(m) * df[atrcol]
                    df[f'KC_DN_{p}_{m}x_ATR{a}'] = mid - float(m) * df[atrcol]

    # SuperTrend bands
    if "SuperTrend" in families_selected:
        stc = cfg_sets.get("SuperTrend", {"atr_periods":[10], "multipliers":[2.0]})
        apers = sorted(set(int(x) for x in stc.get("atr_periods", []) or []))
        mults = sorted(set(float(x) for x in stc.get("multipliers", []) or []))
        hl2 = (high + low) / 2.0
        for a in apers:
            atrcol = f'ATR{a}'
            if atrcol not in df.columns and "TR" in df.columns:
                df[atrcol] = df['TR'].shift(1).ewm(alpha=1/max(a,1), adjust=False).mean()
            for m in mults:
                ub = (hl2 + float(m) * df[atrcol]).shift(1)
                lb = (hl2 - float(m) * df[atrcol]).shift(1)
                df[f'ST_UB_ATR{a}_{m}x'] = ub
                df[f'ST_LB_ATR{a}_{m}x'] = lb

    # MACD
    if "MACD" in families_selected:
        mc = cfg_sets.get("MACD", {"fast":[12], "slow":[26], "signal":[9]})
        fasts = sorted(set(int(x) for x in mc.get("fast", []) or []))
        slows = sorted(set(int(x) for x in mc.get("slow", []) or []))
        sigs  = sorted(set(int(x) for x in mc.get("signal", []) or []))
        for f in fasts:
            ema_fast = close.shift(1).ewm(span=f, adjust=False).mean()
            for s in slows:
                ema_slow = close.shift(1).ewm(span=s, adjust=False).mean()
                macd_line = ema_fast - ema_slow
                for g in sigs:
                    macd_sig = macd_line.shift(1).ewm(span=g, adjust=False).mean()
                    df[f'MACD_{f}_{s}_{g}_line'] = macd_line
                    df[f'MACD_{f}_{s}_{g}_signal'] = macd_sig
                    df[f'MACD_{f}_{s}_{g}_hist'] = macd_line - macd_sig

    # OBV
    if "OBV" in families_selected:
        sign = np.sign(close.diff().fillna(0.0))
        vol = df.get('volume', pd.Series(0.0, index=df.index))
        df['OBV'] = (sign * vol).cumsum()

    # Aroon — NEW
    if "Aroon" in families_selected:
        for n in sorted(set(int(x) for x in (cfg_sets.get("Aroon", []) or []))):
            # periods since highest high / lowest low over last n (using prior data)
            hh_idx = high.shift(1).rolling(n, min_periods=n).apply(lambda x: (n-1) - int(np.argmax(x)), raw=True)
            ll_idx = low.shift(1).rolling(n, min_periods=n).apply(lambda x: (n-1) - int(np.argmin(x)), raw=True)
            df[f'AroonUp_{n}'] = 100.0 * (n - hh_idx) / n
            df[f'AroonDn_{n}'] = 100.0 * (n - ll_idx) / n
            df[f'AroonOsc_{n}'] = df[f'AroonUp_{n}'] - df[f'AroonDn_{n}']

    # CCI — NEW
    if "CCI" in families_selected:
        tp = (high + low + close) / 3.0
        for n in sorted(set(int(x) for x in (cfg_sets.get("CCI", []) or []))):
            ma = tp.shift(1).rolling(n, min_periods=n).mean()
            md = tp.shift(1).rolling(n, min_periods=n).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
            df[f'CCI_{n}'] = (tp - ma) / (0.015 * md.replace(0, np.nan))

    # Price Z-Score — NEW
    if "ZPRICE" in families_selected:
        for n in sorted(set(int(x) for x in (cfg_sets.get("ZPRICE", []) or []))):
            mu = close.shift(1).rolling(n, min_periods=n).mean()
            sd = close.shift(1).rolling(n, min_periods=n).std(ddof=0)
            df[f'ZPRICE_{n}'] = (close - mu) / sd.replace(0, np.nan)

    # Return Z-Score — NEW
    if "ZRET" in families_selected:
        r = close.pct_change()
        for n in sorted(set(int(x) for x in (cfg_sets.get("ZRET", []) or []))):
            mu = r.shift(1).rolling(n, min_periods=n).mean()
            sd = r.shift(1).rolling(n, min_periods=n).std(ddof=0)
            df[f'ZRET_{n}'] = (r - mu) / sd.replace(0, np.nan)

    keep = ["time","Date","open","high","low","close","volume"]
    computed_cols = [c for c in df.columns if c not in keep]
    keep += computed_cols
    out = df[keep].copy()
    if scope == "latest" and not out.empty:
        out = out.tail(1)
    return out

def _ind_worker(args):
    sym, bars, families, sets_cfg, scope = args
    try:
        df = compute_individual_indicators(bars, families, sets_cfg, scope)
        if df is not None and not df.empty:
            return (sym, df)
    except Exception:
        return None
    return None

def compute_individual_for_subset(store_dict: dict, families: set, sets_cfg: dict, scope: str) -> dict:
    out = {}
    items = list(store_dict.items())
    if not items or not families: return out
    cfg_local = st.session_state.get("cfg", {})
    use_proc = bool(cfg_local.get("ind_use_processes", False))
    max_workers = int(cfg_local.get("ind_workers", min(4, max(1, os.cpu_count() or 2))))
    max_workers = max(1, min(16, max_workers))
    results = []
    if use_proc:
        try:
            ctx = get_context("spawn")
            with ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as ex:
                futs = [ex.submit(_ind_worker, (sym, bars, families, sets_cfg, scope)) for sym, bars in items]
                for f in as_completed(futs):
                    res = f.result()
                    if res is not None: results.append(res)
        except Exception:
            with ThreadPoolExecutor(max_workers=min(8, max_workers)) as ex:
                futs = [ex.submit(_ind_worker, (sym, bars, families, sets_cfg, scope)) for sym, bars in items]
                for f in as_completed(futs):
                    res = f.result()
                    if res is not None: results.append(res)
    else:
        try:
            with ThreadPoolExecutor(max_workers=min(8, max_workers)) as ex:
                futs = [ex.submit(_ind_worker, (sym, bars, families, sets_cfg, scope)) for sym, bars in items]
                for f in as_completed(futs):
                    res = f.result()
                    if res is not None: results.append(res)
        except Exception:
            for sym, bars in items:
                res = _ind_worker((sym, bars, families, sets_cfg, scope))
                if res is not None: results.append(res)
    for sym, df in results: out[sym] = df
    return out

# =============================================================================
# 10) DuckDB: schema & helpers
# =============================================================================
try:
    import duckdb
except Exception:
    duckdb = None

def _duckdb_path() -> str:
    root = st.session_state.cfg.get("store_root", st.session_state.store_root)
    ensure_dir(root)
    return os.path.join(root, st.session_state.cfg.get("db_filename", "market_data.duckdb"))

def _duckdb_connect():
    if duckdb is None:
        st.error("duckdb is not installed. pip install duckdb", icon="🚫")
        return None
    con = duckdb.connect(_duckdb_path())
    con.execute("PRAGMA threads=%s" % max(1, (os.cpu_count() or 2)))
    con.execute("PRAGMA enable_object_cache=true")
    return con

def _duckdb_init(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS timeseries (
            symbol TEXT,
            symbol_id BIGINT,
            "interval" TEXT,
            "time" BIGINT,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
            source TEXT,
            normalized_api_factor DOUBLE,
            market_or_pair TEXT,
            market_or_pair_id BIGINT,
            is_inter BOOLEAN,
            strategy TEXT,
            strategy_id BIGINT,
            tenor_months INTEGER,
            contract_tag TEXT,
            curve_bucket TEXT,
            calc_method TEXT,
            dataset_kind TEXT,
            PRIMARY KEY (symbol, "interval", calc_method, "time"),
            CHECK (high >= low),
            CHECK ("time" > 0)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS indicators (
            symbol TEXT,
            symbol_id BIGINT,
            "interval" TEXT,
            "time" BIGINT,
            source TEXT,
            market_or_pair TEXT,
            market_or_pair_id BIGINT,
            is_inter BOOLEAN,
            strategy TEXT,
            strategy_id BIGINT,
            tenor_months INTEGER,
            contract_tag TEXT,
            curve_bucket TEXT,
            calc_method TEXT,
            dataset_kind TEXT,
            PRIMARY KEY (symbol, "interval", calc_method, "time"),
            CHECK ("time" > 0)
        );
    """)
    con.execute("""CREATE TABLE IF NOT EXISTS dim_symbol (id BIGINT PRIMARY KEY, symbol TEXT UNIQUE);""")
    con.execute("""CREATE TABLE IF NOT EXISTS dim_strategy (id BIGINT PRIMARY KEY, strategy TEXT UNIQUE);""")
    con.execute("""CREATE TABLE IF NOT EXISTS dim_source (id BIGINT PRIMARY KEY, calc_method TEXT UNIQUE);""")
    con.execute("""CREATE TABLE IF NOT EXISTS dim_market (id BIGINT PRIMARY KEY, market_or_pair TEXT UNIQUE);""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS scales_audit (
            symbol TEXT, "interval" TEXT, strategy TEXT,
            factor DOUBLE, overlap_count INTEGER, mape DOUBLE, accepted BOOLEAN,
            reason TEXT, run_ts BIGINT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS scale_lookup (
            symbol TEXT, "interval" TEXT, calc_method TEXT, factor DOUBLE, updated_at_utc TEXT,
            PRIMARY KEY (symbol, "interval", calc_method)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS run_meta (
            run_id TEXT, created_at_utc TEXT, host TEXT, app_version TEXT,
            universe_json TEXT, config_json TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS contracts_catalog (
            symbol TEXT PRIMARY KEY,
            base_product TEXT,
            market_or_pair TEXT,
            is_inter BOOLEAN,
            strategy TEXT,
            tenor_months INTEGER,
            contract_tag TEXT,
            curve_bucket TEXT,
            left_leg TEXT,
            mid_leg TEXT,
            right_leg TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS loaded_contracts (
            market_or_pair TEXT,
            is_inter BOOLEAN,
            symbol TEXT,
            "interval" TEXT,
            source TEXT,
            strategy TEXT,
            tenor_months INTEGER,
            contract_tag TEXT,
            curve_bucket TEXT,
            left_leg TEXT,
            mid_leg TEXT,
            right_leg TEXT,
            PRIMARY KEY (market_or_pair, "interval", source, symbol)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            id INTEGER PRIMARY KEY,
            settings_json TEXT,
            updated_at_utc TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS freshness_sla (
            symbol TEXT, "interval" TEXT, calc_method TEXT,
            last_time BIGINT, age_seconds DOUBLE, status TEXT, threshold_seconds INTEGER, updated_at_utc TEXT,
            PRIMARY KEY (symbol, "interval", calc_method)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY, started_at_utc TEXT, ended_at_utc TEXT,
            rows_written BIGINT, sha256 TEXT
        );
    """)

    # Views: include asof_utc (proper timestamp) for both timeseries and joined view
    con.execute("""
        CREATE OR REPLACE VIEW v_timeseries_all AS
        SELECT symbol, symbol_id, "interval", "time",
               to_timestamp("time" / 1000.0) AS asof_utc,
               open, high, low, close, COALESCE(volume,0.0) AS volume,
               source, normalized_api_factor,
               market_or_pair, market_or_pair_id, is_inter, strategy, strategy_id, tenor_months, contract_tag, curve_bucket,
               calc_method, dataset_kind
        FROM timeseries;
    """)
    con.execute("""CREATE OR REPLACE VIEW v_indicators_all AS SELECT * FROM indicators;""")
    con.execute("""
        CREATE OR REPLACE VIEW v_timeseries_with_indicators AS
        SELECT
            t.symbol,
            t.symbol_id,
            t."interval",
            t."time",
            to_timestamp(t."time"/1000.0) AS asof_utc,
            t.calc_method AS calc_method,
            t.calc_method AS source,
            t.open, t.high, t.low, t.close, COALESCE(t.volume,0.0) AS volume,
            t.normalized_api_factor,
            t.market_or_pair, t.market_or_pair_id, t.is_inter, t.strategy, t.strategy_id, t.tenor_months,
            t.contract_tag, t.curve_bucket, t.dataset_kind,
            i.* EXCLUDE (symbol, symbol_id, "interval", "time", source, market_or_pair, market_or_pair_id, is_inter,
                          strategy, strategy_id, tenor_months, contract_tag, curve_bucket, calc_method, dataset_kind)
        FROM timeseries t
        LEFT JOIN indicators i
          ON t.symbol = i.symbol AND t."interval" = i."interval" AND t."time" = i."time" AND t.calc_method = i.calc_method;
    """)

def _duckdb_table_columns(con, table: str) -> set:
    return set(row[1] for row in con.execute(f"PRAGMA table_info('{table}')").fetchall())

def _duckdb_add_missing_columns(con, table: str, df: pd.DataFrame):
    existing = _duckdb_table_columns(con, table)
    for col in df.columns:
        if col in existing: continue
        dtype = "DOUBLE"
        if col in ("symbol","interval","source","strategy","reason","market_or_pair","left_leg","mid_leg","right_leg","base_product","contract_tag","dataset_kind","calc_method","curve_bucket"):
            dtype = "TEXT"
        if col in ("time","run_ts","overlap_count") or col.endswith("_id"):
            dtype = "BIGINT"
        if col in ("is_inter","accepted"):
            dtype = "BOOLEAN"
        if col in ("tenor_months",):
            dtype = "INTEGER"
        if col == "Date":
            dtype = "TIMESTAMP"
        con.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "{col}" {dtype};')

def _duckdb_upsert_df(con, table: str, df: pd.DataFrame, key_cols: list):
    if df is None or df.empty:
        return
    if "Date" in df.columns:
        df = df.drop(columns=["Date"])
    df = df.drop_duplicates(subset=key_cols, keep='last').copy()
    _duckdb_add_missing_columns(con, table, df)
    table_cols = _duckdb_table_columns(con, table)
    cols = [c for c in df.columns if c in table_cols]
    if not cols:
        return
    con.register("df_upsert", df[cols])
    col_list = ", ".join(f'"{c}"' for c in cols)
    try:
        con.execute("BEGIN;")
        con.execute(f'INSERT OR REPLACE INTO {table} ({col_list}) SELECT {col_list} FROM df_upsert;')
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        key_cols_quoted = ", ".join(f'"{k}"' for k in key_cols)
        non_key_cols = [c for c in cols if c not in key_cols]
        update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols)
        on_conflict_sql = (
            f'INSERT INTO {table} ({col_list}) SELECT {col_list} FROM df_upsert '
            f'ON CONFLICT ({key_cols_quoted}) '
            + (f'DO UPDATE SET {update_clause}' if update_clause else 'DO NOTHING')
            + ';'
        )
        try:
            con.execute("BEGIN;")
            con.execute(on_conflict_sql)
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            con.execute("BEGIN;")
            con.execute(
                f"CREATE TEMP TABLE tmp_upsert AS SELECT {col_list}, row_number() OVER () AS rn FROM df_upsert;"
            )
            con.execute(
                f"CREATE TEMP TABLE tmp_upsert_dedup AS SELECT DISTINCT ON ({key_cols_quoted}) {col_list} "
                f"FROM tmp_upsert ORDER BY {key_cols_quoted}, rn DESC;"
            )
            con.execute(f"CREATE TEMP TABLE tmp_keys AS SELECT DISTINCT {key_cols_quoted} FROM tmp_upsert_dedup;")
            join_cond = " AND ".join([f't."{k}" = k."{k}"' for k in key_cols])
            con.execute(f'DELETE FROM {table} t USING tmp_keys k WHERE {join_cond};')
            con.execute(f'INSERT INTO {table} ({col_list}) SELECT {col_list} FROM tmp_upsert_dedup;')
            con.execute("DROP TABLE tmp_upsert;")
            con.execute("DROP TABLE tmp_upsert_dedup;")
            con.execute("DROP TABLE tmp_keys;")
            con.execute("COMMIT;")
    finally:
        try:
            con.unregister("df_upsert")
        except Exception:
            pass

def db_load_settings():
    path = _duckdb_path()
    if not os.path.exists(path): return None
    con = _duckdb_connect()
    if con is None: return None
    try:
        _duckdb_init(con)
        row = con.execute("SELECT settings_json FROM app_settings WHERE id=1;").fetchone()
        if not row or not row[0]: return None
        return json.loads(row[0])
    except Exception:
        return None
    finally:
        try: con.close()
        except Exception: pass

def db_save_settings(cfg: dict):
    con = _duckdb_connect()
    if con is None: return
    try:
        _duckdb_init(con)
        payload = {"settings_json": json.dumps(cfg, separators=(",",":")), "updated_at_utc": datetime.utcnow().isoformat()}
        df = pd.DataFrame([{**payload, "id": 1}])
        _duckdb_upsert_df(con, "app_settings", df, ["id"])
    finally:
        try: con.close()
        except Exception: pass

# =============================================================================
# 11) Settings UI
# =============================================================================
def _parse_int_list(txt, default_list):
    try:
        vals = [int(x.strip()) for x in str(txt).replace(";",",").split(",") if str(x).strip()!=""]
        return sorted(set(vals)) if vals else list(default_list)
    except Exception:
        return list(default_list)

def _parse_float_list(txt, default_list):
    try:
        vals = [float(x.strip()) for x in str(txt).replace(";",",").split(",") if str(x).strip()!=""]
        return sorted(set(vals)) if vals else list(default_list)
    except Exception:
        return list(default_list)

def render_settings():
    st.subheader("Settings")
    tabs = st.tabs([
        "General", "Universe", "Indicator Sets", "Global Indicators", "Per-Product", "Intermarket Pairs",
        "Reconcile & Scale", "Persistence & Refresh", "Status & Health", "DB Browser", "Advanced & UI"
    ])
    cfg = st.session_state.cfg.copy()

    # General
    with tabs[0]:
        st.markdown("### API Credentials")
        col1, col2 = st.columns([3,1])
        with col1:
            token = st.text_input("API Token", value=cfg.get("token", st.session_state.access_token), type="password")
        with col2:
            scheme = st.selectbox("Auth Scheme", ["Bearer","JWT","Raw (exact header)"],
                                  index=["Bearer","JWT","Raw (exact header)"].index(cfg.get("scheme","Bearer")))
        st.markdown("### Synthetic Rules")
        c1,c2,c3,c4 = st.columns(4)
        _pair_tol_min    = int(round(cfg.get("pair_tol", DEFAULT_PAIR_TOLERANCE_SECONDS)/60))
        _span_tol_min    = int(round(cfg.get("span_tol", DEFAULT_SPAN_TOLERANCE_SECONDS)/60))
        _max_leg_age_min = int(round(cfg.get("max_leg_age", DEFAULT_MAX_LEG_AGE_SECONDS)/60))
        _stale_1m_min    = int(round(cfg.get("stale_1m", DEFAULT_STALE_1M_SECONDS)/60))
        pair_tol_min    = c1.number_input("Per-pair tolerance (minutes)", 0, 7*24*60, _pair_tol_min, 1)
        span_tol_min    = c2.number_input("Total span tolerance (minutes)", 0, 7*24*60, _span_tol_min, 5)
        max_leg_age_min = c3.number_input("Max leg age (minutes)", 0, 7*24*60, _max_leg_age_min, 5)
        stale_1m_min    = c4.number_input("Stale API 1M override age (minutes)", 0, 14*24*60, _stale_1m_min, 30)

        st.markdown("### Structure Generation")
        cc1, cc2, cc3 = st.columns(3)
        num_consecutive = cc1.number_input("Consecutive months", 0, 36, int(cfg.get("num_consecutive", 15)), 1)
        num_quarterly   = cc2.number_input("Additional quarterly years", 0, 10, int(cfg.get("num_quarterly", 4)), 1)
        tenors_sel = cfg.get("tenors", [1,2,3,6,9,12])
        tenors = cc3.multiselect("Tenors (months)", [1,2,3,6,9,12,18,24], tenors_sel)
        cfg.update({
            "token": token, "scheme": scheme,
            "pair_tol": int(pair_tol_min)*60,
            "span_tol": int(span_tol_min)*60,
            "max_leg_age": int(max_leg_age_min)*60,
            "stale_1m": int(stale_1m_min)*60,
            "num_consecutive": int(num_consecutive), "num_quarterly": int(num_quarterly), "tenors": tenors
        })

    # Universe
    with tabs[1]:
        st.markdown("### Products & Intermarket Pairs")
        products = st.multiselect("Products", ALL_PRODUCT_CODES, cfg.get("products", ALL_PRODUCT_CODES))
        pairs_all = [f"{a}-{b}" for a,b in ALL_INTERMARKET_PAIRS]
        pairs = st.multiselect("Intermarket Pairs", pairs_all, cfg.get("pairs", pairs_all))
        cfg.update({"products": products, "pairs": pairs})

    # Indicator Sets
    with tabs[2]:
        st.markdown("Default parameter sets for indicator families.")
        sets_cfg = cfg.get("indicator_sets", DEFAULT_INDICATOR_SETS)

        colA, colB, colC = st.columns(3)
        with colA:
            ema_list = st.text_input("EMA periods", value=",".join(str(x) for x in sets_cfg.get("EMA", [20,50,100])))
            atr_list = st.text_input("ATR periods", value=",".join(str(x) for x in sets_cfg.get("ATR", [10,14,20])))
            rsi_list = st.text_input("RSI periods", value=",".join(str(x) for x in sets_cfg.get("RSI", [14])))
            adx_list = st.text_input("ADX periods", value=",".join(str(x) for x in sets_cfg.get("ADX", [14])))
            sma_list = st.text_input("SMA periods", value=",".join(str(x) for x in sets_cfg.get("SMA", [5,10,20,30,50,100])))
        with colB:
            rng_list = st.text_input("Ranges lookback", value=",".join(str(x) for x in sets_cfg.get("Ranges", [5,10,20])))
            ra_ranges = st.text_input("Range/ATR — ranges", value=",".join(str(x) for x in sets_cfg.get("Range/ATR", {}).get("ranges", [20])))
            ra_atrs   = st.text_input("Range/ATR — ATRs", value=",".join(str(x) for x in sets_cfg.get("Range/ATR", {}).get("atrs", [20])))
            bb_periods = st.text_input("BB — periods", value=",".join(str(x) for x in sets_cfg.get("BB", {}).get("periods", [20])))
            aroon_list = st.text_input("Aroon periods", value=",".join(str(x) for x in sets_cfg.get("Aroon", [14])))
        with colC:
            bb_stds = st.text_input("BB — std multipliers", value=",".join(str(x) for x in sets_cfg.get("BB", {}).get("stds", [2.0])))
            kc_periods = st.text_input("KC — EMA periods", value=",".join(str(x) for x in sets_cfg.get("KC", {}).get("periods", [20])))
            kc_mults   = st.text_input("KC — multipliers", value=",".join(str(x) for x in sets_cfg.get("KC", {}).get("multipliers", [2.0])))
            kc_atrs    = st.text_input("KC — ATR periods", value=",".join(str(x) for x in sets_cfg.get("KC", {}).get("atr_periods", [20])))
            cci_list   = st.text_input("CCI periods", value=",".join(str(x) for x in sets_cfg.get("CCI", [20])))
            zp_list    = st.text_input("Price Z-score windows", value=",".join(str(x) for x in sets_cfg.get("ZPRICE", [20])))
            zr_list    = st.text_input("Return Z-score windows", value=",".join(str(x) for x in sets_cfg.get("ZRET", [20])))

        st.markdown("**SuperTrend / MACD / Range/Range**")
        st1, st2, mc1, mc2, mc3, r2a, r2b = st.columns(7)
        st_ap = st1.text_input("ST — ATR periods", value=",".join(str(x) for x in sets_cfg.get("SuperTrend", {}).get("atr_periods", [10])))
        st_mu = st2.text_input("ST — multipliers", value=",".join(str(x) for x in sets_cfg.get("SuperTrend", {}).get("multipliers", [2.0])))
        mc_fast = mc1.text_input("MACD — fast", value=",".join(str(x) for x in sets_cfg.get("MACD", {}).get("fast", [12])))
        mc_slow = mc2.text_input("MACD — slow", value=",".join(str(x) for x in sets_cfg.get("MACD", {}).get("slow", [26])))
        mc_sig  = mc3.text_input("MACD — signal", value=",".join(str(x) for x in sets_cfg.get("MACD", {}).get("signal", [9])))
        rr_nums = r2a.text_input("Range/Range — numerators (m)", value=",".join(str(x) for x in sets_cfg.get("Range/Range", {}).get("numerators", [20])))
        rr_dens = r2b.text_input("Range/Range — denominators (n)", value=",".join(str(x) for x in sets_cfg.get("Range/Range", {}).get("denominators", [10])))

        upd_sets = {
            "TR": True,
            "EMA": _parse_int_list(ema_list, [20,50,100]),
            "ATR": _parse_int_list(atr_list, [10,14,20]),
            "RSI": _parse_int_list(rsi_list, [14]),
            "ADX": _parse_int_list(adx_list, [14]),
            "SMA": _parse_int_list(sma_list, [5,10,20,30,50,100]),
            "Ranges": _parse_int_list(rng_list, [5,10,20]),
            "Range/ATR": {"ranges": _parse_int_list(ra_ranges, [20]), "atrs": _parse_int_list(ra_atrs, [20])},
            "Range/Range": {"numerators": _parse_int_list(rr_nums, [20]), "denominators": _parse_int_list(rr_dens, [10])},
            "BB": {"periods": _parse_int_list(bb_periods, [20]), "stds": _parse_float_list(bb_stds, [2.0])},
            "KC": {"periods": _parse_int_list(kc_periods, [20]), "multipliers": _parse_float_list(kc_mults, [2.0]), "atr_periods": _parse_int_list(kc_atrs, [20])},
            "SuperTrend": {"atr_periods": _parse_int_list(st_ap, [10]), "multipliers": _parse_float_list(st_mu, [2.0])},
            "MACD": {"fast": _parse_int_list(mc_fast, [12]), "slow": _parse_int_list(mc_slow, [26]), "signal": _parse_int_list(mc_sig, [9])},
            "OBV": True,
            "Aroon": _parse_int_list(aroon_list, [14]),
            "CCI": _parse_int_list(cci_list, [20]),
            "ZPRICE": _parse_int_list(zp_list, [20]),
            "ZRET": _parse_int_list(zr_list, [20]),
        }
        cfg["indicator_sets"] = upd_sets

    # Global Indicators
    with tabs[3]:
        st.markdown("### Global Indicator Override")
        gi = cfg.get("global_ind", {"enabled": False, "apply_to": ["Markets","Intermarket"], "families": ["ALL"], "scope": "full"})
        enabled = st.checkbox("Enable global indicator override", value=gi.get("enabled", False))
        apply_to = st.multiselect("Apply to", ["Markets","Intermarket"], gi.get("apply_to", ["Markets","Intermarket"]))
        fams = st.multiselect("Indicator families", INDICATOR_FAMILY_ALL, gi.get("families", ["ALL"]))
        scope = st.selectbox("Scope", ["full","latest"], index=["full","latest"].index(gi.get("scope","full")))
        cfg["global_ind"] = {"enabled": bool(enabled), "apply_to": apply_to, "families": fams, "scope": scope}

    # Per-Product
    with tabs[4]:
        st.markdown("Per-product intervals & indicators.")
        p_cfg = cfg.get("per_product", {})
        for code in cfg.get("products", []):
            with st.expander(f"{code} configuration", expanded=False):
                p = p_cfg.get(code, {})
                intervals = p.get("intervals", {
                    "1M": {"enabled": True, "refresh_minutes": 5},
                    "1D": {"enabled": True, "count": 200, "refresh_hours": 6, "batch_limit": 50, "timeout": 60},
                    "1H": {"enabled": True, "count": 950, "refresh_hours": 3, "batch_limit": 10, "timeout": 70},
                    "5M": {"enabled": True, "count": 100, "refresh_hours": 1, "batch_limit": 30, "timeout": 60},
                })
                st.markdown("**1M Latest**")
                i1 = intervals["1M"]
                colA, colB = st.columns(2)
                i1["enabled"] = colA.checkbox(f"{code} · Enable 1M", value=i1.get("enabled", True), key=f"{code}_1m_en")
                i1["refresh_minutes"] = colB.number_input(f"{code} · 1M refresh (min)", 1, 240, int(i1.get("refresh_minutes", 5)), 1, key=f"{code}_1m_rf")
                for itv in ["1D","1H","5M"]:
                    st.markdown(f"**{itv}**")
                    row = intervals[itv]
                    c1,c2,c3,c4,c5 = st.columns([1,1,1,1,1])
                    row["enabled"] = c1.checkbox(f"Enable {itv}", value=row.get("enabled", True), key=f"{code}_{itv}_en")
                    row["count"] = c2.number_input(f"{itv} count", 10, 20000, int(row.get("count", 200)), 10, key=f"{code}_{itv}_ct")
                    row["refresh_hours"] = c3.number_input(f"{itv} refresh (h)", 1, 168, int(row.get("refresh_hours", 6)), 1, key=f"{code}_{itv}_rf")
                    row["batch_limit"] = c4.number_input(f"{itv} batch", 1, MAX_INSTRUMENTS_PER_REQUEST, int(row.get("batch_limit", 50)), 1, key=f"{code}_{itv}_bl")
                    row["timeout"] = c5.number_input(f"{itv} timeout (s)", 10, 600, int(row.get("timeout", 60)), 5, key=f"{code}_{itv}_to")
                    intervals[itv] = row
                st.markdown("**Indicator families per interval**")
                ind = p.get("indicators", {})
                for itv in ["1D","1H","5M"]:
                    ic = ind.get(itv, {"selection": [], "scope": "full"})
                    d1, d2 = st.columns([3,1])
                    sel = d1.multiselect(f"{code} · {itv} families", INDICATOR_FAMILY_ALL, ic.get("selection", []), key=f"{code}_{itv}_ind")
                    scope = d2.selectbox(f"{itv} calc scope", ["full","latest"], ["full","latest"].index(ic.get("scope","full")), key=f"{code}_{itv}_scp")
                    ind[itv] = {"selection": sel, "scope": scope}
                p["intervals"] = intervals
                p["indicators"] = ind
                p_cfg[code] = p
        cfg["per_product"] = p_cfg

    # Intermarket Pairs
    with tabs[5]:
        st.markdown("Select families for intermarket indicators.")
        im_cfg = cfg.get("intermarket", {})
        for pair_s in cfg.get("pairs", []):
            with st.expander(f"{pair_s} indicators", expanded=False):
                ic = im_cfg.get(pair_s, {})
                for itv in ["1D","1H","5M"]:
                    row = ic.get(itv, {"selection": [], "scope": "full"})
                    c1,c2 = st.columns([3,1])
                    sel = c1.multiselect(f"{pair_s} · {itv} families", INDICATOR_FAMILY_ALL, row.get("selection", []), key=f"{pair_s}_{itv}_ind")
                    scope = c2.selectbox(f"{itv} scope", ["full","latest"], ["full","latest"].index(row.get("scope","full")), key=f"{pair_s}_{itv}_scp")
                    ic[itv] = {"selection": sel, "scope": scope}
                im_cfg[pair_s] = ic
        cfg["intermarket"] = im_cfg

    # Reconcile & Scale
    with tabs[6]:
        st.markdown("### Reconciliation / Normalization")
        abs_thr = st.number_input("ABS threshold", 0.0, 1e15, float(cfg.get("abs_thr", DEFAULT_ABS_RECON_THR)), format="%.0f")
        rel_thr = st.number_input("REL threshold", 0.0, 10.0, float(cfg.get("rel_thr", DEFAULT_REL_RECON_THR)), format="%.3f")
        mape_tol = st.number_input("MAPE tolerance (scale accept)", 0.0, 1.0, float(cfg.get("mape_tol", 0.5)), format="%.4f")
        min_overlap = st.number_input("Min overlap for scaling", 0, 10000, int(cfg.get("min_overlap", 5)), 1)
        scale_overrides = st.text_area("Manual scale overrides (JSON: {'symbol|interval|strategy': factor})",
                                       value=cfg.get("scale_overrides","{}"), height=120)
        cfg.update({"abs_thr": float(abs_thr), "rel_thr": float(rel_thr),
                    "mape_tol": float(mape_tol), "min_overlap": int(min_overlap),
                    "scale_overrides": scale_overrides})

    # Persistence & Refresh
    with tabs[7]:
        st.markdown("### Database (DuckDB) & Display")
        store_root = st.text_input("Store root folder", value=cfg.get("store_root", st.session_state.store_root))
        db_filename = st.text_input("DB filename", value=cfg.get("db_filename","market_data.duckdb"))
        autosave = st.checkbox("Auto-write to DB after each refresh", value=cfg.get("autosave", True))
        enable_autorefresh = st.checkbox("Enable auto-refresh", value=cfg.get("enable_autorefresh", True))
        autorefresh_minutes = st.number_input("Auto-refresh period (minutes)", 1, 240, int(cfg.get("autorefresh_minutes", 5)), 1)

        prefer_db_display = st.checkbox("Prefer DB for display (tables & charts)", value=cfg.get("prefer_db_display", True))
        db_hard_lock = st.checkbox("Hard-lock charts to DB sources only", value=cfg.get("db_hard_lock", True))
        default_chart_source = st.selectbox("Default chart data source",
                                            ["Auto","API","SYN","DB API","DB SYN"],
                                            index={"auto":0,"api":1,"syn":2,"db_api":3,"db_syn":4}.get(cfg.get("default_chart_source","auto"),0))
        max_symbols_controls = st.number_input("Max rows per section", 10, 500, int(cfg.get("max_symbols_controls", 120)), 10)

        st.markdown("### Parquet Mirror (partitioned)")
        export_parquet = st.checkbox("Export Parquet mirror on autosave", value=cfg.get("export_parquet", True))
        parquet_root = st.text_input("Parquet root folder", value=cfg.get("parquet_root", os.path.join(store_root, "parquet_mirror")))
        parquet_mode = st.selectbox("Parquet snapshot mode", ["overwrite_current","timestamped"], index=["overwrite_current","timestamped"].index(cfg.get("parquet_mode","overwrite_current")))
        parquet_compression = st.selectbox("Parquet compression", ["ZSTD","SNAPPY","GZIP","UNCOMPRESSED"], index=["ZSTD","SNAPPY","GZIP","UNCOMPRESSED"].index(cfg.get("parquet_compression","ZSTD")))

        snapshot_on_save = st.checkbox("Create DuckDB snapshot on autosave", value=cfg.get("snapshot_on_save", True))
        snapshot_root = st.text_input("Snapshot folder", value=cfg.get("snapshot_root", os.path.join(store_root, "db_snapshots")))
        snapshot_minutes = st.number_input("Min minutes between snapshots", 1, 1440, int(cfg.get("snapshot_minutes", 30)), 1)

        cfg.update({"store_root": store_root, "db_filename": db_filename, "autosave": autosave,
                    "enable_autorefresh": enable_autorefresh, "autorefresh_minutes": int(autorefresh_minutes),
                    "prefer_db_display": bool(prefer_db_display),
                    "db_hard_lock": bool(db_hard_lock),
                    "default_chart_source": { "Auto":"auto", "API":"api", "SYN":"syn", "DB API":"db_api", "DB SYN":"db_syn"}[default_chart_source],
                    "max_symbols_controls": int(max_symbols_controls),
                    "export_parquet": bool(export_parquet),
                    "parquet_root": parquet_root, "parquet_mode": parquet_mode, "parquet_compression": parquet_compression,
                    "snapshot_on_save": bool(snapshot_on_save), "snapshot_root": snapshot_root, "snapshot_minutes": int(snapshot_minutes)})

    # Status & Health
    with tabs[8]:
        fresh_minutes = st.number_input("Minutes threshold for 🟢 Fresh", 1, 1440, int(cfg.get("fresh_minutes", DEFAULT_FRESH_MINUTES)), 1)
        cfg["fresh_minutes"] = int(fresh_minutes)

    # DB Browser
    with tabs[9]:
        st.markdown("### DB Browser / Loader")
        path = _duckdb_path()
        st.write("DB file:", path)
        con = _duckdb_connect()
        if con:
            _duckdb_init(con)
            try:
                st.success("Connected to DuckDB")
            except Exception:
                st.error("DuckDB connection failed")
            tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
            st.write("Tables:", tables)

            st.markdown("#### Partition Loader")
            colf = st.columns(10)
            with colf[0]: market_or_pair = st.text_input("Market / Pair")
            with colf[1]: strategy = st.selectbox("Strategy", ["(any)","outright","spread","fly","inter_outright","inter_spread","inter_fly"], index=0)
            with colf[2]: tenor = st.text_input("Tenor months (e.g. 3)")
            with colf[3]: contract_tag = st.text_input("Contract Tag (e.g. spread_3M)")
            with colf[4]: curve_bucket = st.selectbox("Curve bucket", ["(any)","front","mid","back"], index=0)
            with colf[5]: interval_sel = st.selectbox("Interval", ["(any)","1M","1D","1H","5M"], index=0)
            with colf[6]: calc_method = st.selectbox("Calc Method", ["(any)","api","syn"], index=0)
            with colf[7]: dataset_kind = st.selectbox("Dataset Kind", ["onlytimeseries","withindicators"], index=0)
            with colf[8]: contract = st.text_input("Contract symbol")
            with colf[9]: limit_rows = st.number_input("Limit", 10, 100000, 2000, 10)
            run = st.button("Load from DB")
            if run:
                base = "SELECT * FROM v_timeseries_with_indicators" if dataset_kind == "withindicators" else "SELECT * FROM v_timeseries_all"
                where = []; params = []
                if market_or_pair.strip(): where.append('market_or_pair = ?'); params.append(market_or_pair.strip())
                if strategy != "(any)": where.append('strategy = ?'); params.append(strategy)
                if tenor.strip():
                    try: where.append('tenor_months = ?'); params.append(int(tenor.strip()))
                    except Exception: st.error("Tenor must be integer months.")
                if contract_tag.strip(): where.append('contract_tag = ?'); params.append(contract_tag.strip())
                if curve_bucket != "(any)": where.append('curve_bucket = ?'); params.append(curve_bucket)
                if interval_sel != "(any)": where.append('"interval" = ?'); params.append(interval_sel)
                if calc_method != "(any)": where.append('calc_method = ?'); params.append(calc_method)
                if contract.strip(): where.append('symbol = ?'); params.append(contract.strip())
                if where: base += " WHERE " + " AND ".join(where)
                base += " ORDER BY market_or_pair, strategy, tenor_months, curve_bucket, symbol, \"interval\", calc_method, \"time\" LIMIT ?;"
                params.append(int(limit_rows))
                try:
                    df = con.execute(base, params).df()
                    st.dataframe(df, use_container_width=True, height=420)
                except Exception as e:
                    st.error(f"Loader query error: {e}")
            st.markdown("#### Free SQL (read-only recommended)")
            q = st.text_area("SQL", value="SELECT * FROM v_timeseries_with_indicators LIMIT 200;")
            if st.button("Run SQL"):
                try:
                    df = con.execute(q).df()
                    st.dataframe(df, use_container_width=True, height=360)
                except Exception as e:
                    st.error(f"Query error: {e}")
            con.close()

    # Advanced
    with tabs[10]:
        c1, c2 = st.columns(2)
        use_proc = c1.checkbox("Use process pool for indicators (thread-first recommended)", value=cfg.get("ind_use_processes", False))
        workers  = c2.number_input("Indicator workers", 1, 16, int(cfg.get("ind_workers", min(4, max(1, os.cpu_count() or 2)))), 1)
        cfg["ind_use_processes"] = bool(use_proc)
        cfg["ind_workers"] = int(workers)

    st.session_state.cfg = cfg
    c1,c2 = st.columns([1,1])
    if c1.button("Apply settings & Load", use_container_width=True):
        st.session_state.access_token = cfg["token"]
        st.session_state.auth_scheme  = cfg["scheme"]
        st.session_state.store_root   = cfg["store_root"]
        db_save_settings(cfg)
        st.session_state.cfg_applied  = True
        _st_rerun()
    if c2.button("Reset all caches", use_container_width=True):
        st.cache_data.clear(); st.session_state.clear(); _st_rerun()

# =============================================================================
# 12) Boot from DB if present
# =============================================================================
if not st.session_state.cfg_applied:
    cfg_from_db = db_load_settings()
    if cfg_from_db:
        st.session_state.cfg = cfg_from_db
        st.session_state.access_token = cfg_from_db.get("token", st.session_state.access_token)
        st.session_state.auth_scheme = cfg_from_db.get("scheme", st.session_state.auth_scheme)
        st.session_state.store_root = cfg_from_db.get("store_root", st.session_state.store_root)
        st.session_state.cfg_applied = True

# Render settings now (always visible)
main_tabs = st.tabs(["Markets", "Intermarket", "Catalog", "Progress", "Settings"])
with main_tabs[4]:
    render_settings()

if not st.session_state.cfg_applied:
    st.info("Edit settings and press **Apply settings & Load** to start.", icon="🛠️")
    st.stop()

cfg = st.session_state.cfg
DATA_FRESHNESS_THRESHOLD_SECONDS = int(cfg.get("fresh_minutes", DEFAULT_FRESH_MINUTES)) * 60
if cfg.get("enable_autorefresh", True):
    st_autorefresh(interval=cfg.get("autorefresh_minutes", 5)*60*1000, key="auto_refresher")

# =============================================================================
# 13) Build instruments & symbol_meta
# =============================================================================
products = cfg.get("products", [])
pairs_sel = [(p.split("-")[0], p.split("-")[1]) for p in cfg.get("pairs", [])]
num_consecutive = cfg.get("num_consecutive", 15)
num_quarterly   = cfg.get("num_quarterly", 4)
tenors          = cfg.get("tenors", [1,2,3,6,9,12])

contracts_by_product = {}
product_symbols = {code: set() for code in products}
product_outrights = {code: [] for code in products}
for code in products:
    try:
        outr, spr, fly = generate_intra(code, num_consecutive, num_quarterly, tenors)
    except Exception:
        outr, spr, fly = [], {}, {}
    contracts_by_product[code] = {'outr': outr, 'spr': spr, 'fly': fly}
    product_outrights[code] = list(outr)
    product_symbols[code].update(outr)
    for v in spr.values(): product_symbols[code].update(v)
    for v in fly.values(): product_symbols[code].update(v)

inter_defs = generate_inter(contracts_by_product, tenors, pairs_sel)

def _tag(strategy: str, tenor):
    if strategy in ("outright","inter_outright"): return strategy
    if "spread" in strategy and tenor is not None: return ("inter_spread" if strategy.startswith("inter_") else "spread") + f"_{tenor}M"
    if "fly" in strategy and tenor is not None:    return ("inter_fly" if strategy.startswith("inter_") else "fly") + f"_{tenor}M"
    return strategy

symbol_meta = {}
# Intra
for code, defs in contracts_by_product.items():
    for sym in defs['outr']:
        cb = curve_bucket_for_symbol(sym)
        symbol_meta[sym] = dict(market_or_pair=code, is_inter=False, strategy="outright", tenor_months=None,
                                contract_tag=_tag("outright", None), base_product=code,
                                curve_bucket=cb, left_leg=None, mid_leg=None, right_leg=None)
    for t, lst in defs['spr'].items():
        for sym in lst:
            parts = sym.split("-")
            cb = curve_bucket_for_symbol(sym)
            symbol_meta[sym] = dict(market_or_pair=code, is_inter=False, strategy="spread", tenor_months=int(t),
                                    contract_tag=_tag("spread", int(t)), base_product=code,
                                    curve_bucket=cb, left_leg=parts[0], mid_leg=None, right_leg=f"{code}{parts[1]}")
    for t, lst in defs['fly'].items():
        for sym in lst:
            parts = sym.split("-")
            cb = curve_bucket_for_symbol(sym)
            symbol_meta[sym] = dict(market_or_pair=code, is_inter=False, strategy="fly", tenor_months=int(t),
                                    contract_tag=_tag("fly", int(t)), base_product=code,
                                    curve_bucket=cb, left_leg=parts[0], mid_leg=f"{code}{parts[1]}", right_leg=f"{code}{parts[2]}")
# Inter
for (a,b), defs in inter_defs.items():
    key = f"{a}-{b}"
    for sym in defs['outr']:
        l, r = defs['legs_out'][sym]
        cb = curve_bucket_for_symbol(sym)
        symbol_meta[sym] = dict(market_or_pair=key, is_inter=True, strategy="inter_outright", tenor_months=None,
                                contract_tag=_tag("inter_outright", None), base_product=None,
                                curve_bucket=cb, left_leg=l, mid_leg=None, right_leg=r)
    for t, lst in defs['spr'].items():
        for sym in lst:
            i1,i2 = defs['legs_spr'][sym]
            cb = curve_bucket_for_symbol(sym)
            symbol_meta[sym] = dict(market_or_pair=key, is_inter=True, strategy="inter_spread", tenor_months=int(t),
                                    contract_tag=_tag("inter_spread", int(t)), base_product=None,
                                    curve_bucket=cb, left_leg=i1, mid_leg=None, right_leg=i2)
    for t, lst in defs['fly'].items():
        for sym in lst:
            i1,i2,i3 = defs['legs_fly'][sym]
            cb = curve_bucket_for_symbol(sym)
            symbol_meta[sym] = dict(market_or_pair=key, is_inter=True, strategy="inter_fly", tenor_months=int(t),
                                    contract_tag=_tag("inter_fly", int(t)), base_product=None,
                                    curve_bucket=cb, left_leg=i1, mid_leg=i2, right_leg=i3)

st.session_state.symbol_meta = symbol_meta

# =============================================================================
# 14) Fetch API data
# =============================================================================
auth_header_value = build_auth_header_value(st.session_state.access_token, st.session_state.auth_scheme)
pair_tol    = cfg.get("pair_tol", DEFAULT_PAIR_TOLERANCE_SECONDS)
span_tol    = cfg.get("span_tol", DEFAULT_SPAN_TOLERANCE_SECONDS)
max_leg_age = cfg.get("max_leg_age", DEFAULT_MAX_LEG_AGE_SECONDS)
stale_1m    = cfg.get("stale_1m", DEFAULT_STALE_1M_SECONDS)
abs_thr     = cfg.get("abs_thr", DEFAULT_ABS_RECON_THR)
rel_thr     = cfg.get("rel_thr", DEFAULT_REL_RECON_THR)

if 'last_fetch_map' not in st.session_state:
    st.session_state.last_fetch_map = defaultdict(lambda: {'1M':0.0,'1D':0.0,'1H':0.0,'5M':0.0})

def _due(last_ts, refresh_seconds): return (time.time() - last_ts) >= refresh_seconds
st.session_state.progress = {'fetch': defaultdict(dict), 'ind': defaultdict(dict)}

# 1M latest per product
for code in products:
    try:
        pconf = cfg["per_product"][code]["intervals"]["1M"]
    except KeyError:
        continue
    if not pconf.get("enabled", True): continue
    if _due(st.session_state.last_fetch_map[code]['1M'], pconf.get("refresh_minutes",5)*60):
        inst = sorted_calendar(list(product_symbols[code]))
        st.session_state.progress['fetch'][code]['1M'] = {"total": len(inst), "done": 0}
        if inst:
            st.session_state.auth_error = False
            batches = [inst[i:i+MAX_INSTRUMENTS_PER_REQUEST] for i in range(0, len(inst), MAX_INSTRUMENTS_PER_REQUEST)]
            out = {}
            for b in batches:
                m = fetch_latest_1m_batch(",".join(b), auth_header_value)
                if m is None:
                    st.session_state.auth_error = True
                    out = {}
                    break
                out.update(m)
                st.session_state.progress['fetch'][code]['1M']["done"] += len(b)
            if not st.session_state.auth_error:
                ok = validate_api_latest_1m_map(out, st.session_state.withheld)
                st.session_state.prices_1m.update(ok)
        st.session_state.last_fetch_map[code]['1M'] = time.time()

# 1D/1H/5M per product
for code in products:
    try:
        pconf = cfg["per_product"][code]["intervals"]
    except KeyError:
        continue
    for itv in ["1D","1H","5M"]:
        row = pconf[itv]
        if not row.get("enabled", True): continue
        rf_sec = row.get("refresh_hours", 6)*3600
        if not _due(st.session_state.last_fetch_map[code][itv], rf_sec): continue
        syms = sorted_calendar(list(product_symbols[code]))
        total = len(syms)
        st.session_state.progress['fetch'][code][itv] = {"total": total, "done": 0}
        if syms:
            count = row.get("count", 200)
            batch_limit = max(1, min(row.get("batch_limit", 50), MAX_INSTRUMENTS_PER_REQUEST))
            timeout_sec = int(row.get("timeout", 60))
            batches = [syms[i:i+batch_limit] for i in range(0, len(syms), batch_limit)]
            all_res = {}
            for b in batches:
                data = fetch_series_batch(",".join(b), auth_header_value, itv, count, timeout_sec)
                if data is None:
                    st.session_state.auth_error = True; break
                all_res.update(data)
                st.session_state.progress['fetch'][code][itv]["done"] += len(b)
            if all_res is not None and not st.session_state.auth_error:
                st.session_state.auth_error = False
                ok = validate_api_series_dict(all_res, itv, st.session_state.withheld)
                st.session_state.series[itv].update(ok)
        st.session_state.last_fetch_map[code][itv] = time.time()

if st.session_state.auth_error:
    st.error("401 Unauthorized: The API rejected your credentials. No data shown.", icon="🚫")
    st.stop()

st.session_state.last_update = datetime.now().strftime("%I:%M:%S %p")

# =============================================================================
# 15) Synthetic series & latest
# =============================================================================
enabled_series_keys = [k for k in ['1D','1H','5M'] if any(cfg["per_product"].get(code,{}).get("intervals",{}).get(k,{}).get("enabled", True) for code in products)]
series_counts = {
    '1D': max((cfg["per_product"][c]["intervals"]["1D"].get("count",200) for c in products), default=200),
    '1H': max((cfg["per_product"][c]["intervals"]["1H"].get("count",950) for c in products), default=950),
    '5M': max((cfg["per_product"][c]["intervals"]["5M"].get("count",100) for c in products), default=100),
}

def build_intra_synthetic_caches(contracts_by_product, pair_tol, span_tol, max_leg_age, stale_1m_sec, enabled_intervals, counts):
    syn_1m = {}
    syn_series = {k:{} for k in enabled_intervals}
    for code, defs in contracts_by_product.items():
        # spreads
        for t, s_list in defs['spr'].items():
            for sp in s_list:
                l1 = sp.split("-")[0]
                l2 = f"{code}{sp.split('-')[1]}"
                r1 = st.session_state.prices_1m.get(l1)
                r2 = st.session_state.prices_1m.get(l2)
                if r1 and r2 and r1.get("close") is not None and r2.get("close") is not None:
                    syn = weighted_latest([r1,r2], [1,-1], pair_tol, span_tol, max_leg_age)
                    if syn:
                        base = st.session_state.prices_1m.get(sp)
                        kind = "missing"
                        if base: kind = "override" if age_seconds(base.get("time")) > stale_1m_sec else "available"
                        syn_1m[sp] = {"close": syn['close'], "time": syn['time'], "kind": kind}
                for itv in enabled_intervals:
                    cap = counts[itv]
                    s1 = st.session_state.series[itv].get(l1, [])
                    s2 = st.session_state.series[itv].get(l2, [])
                    if s1 and s2:
                        syn_series[itv][sp] = weighted_series([s1,s2], [1,-1], cap)
        # flies
        for t, f_list in defs['fly'].items():
            for fl in f_list:
                p = fl.split("-")
                l1 = p[0]; l2 = f"{code}{p[1]}"; l3 = f"{code}{p[2]}"
                r1 = st.session_state.prices_1m.get(l1)
                r2 = st.session_state.prices_1m.get(l2)
                r3 = st.session_state.prices_1m.get(l3)
                if all([r1,r2,r3]) and all(r.get("close") is not None for r in (r1,r2,r3)):
                    syn = weighted_latest([r1,r2,r3], [1,-2,1], pair_tol, span_tol, max_leg_age)
                    if syn:
                        base = st.session_state.prices_1m.get(fl)
                        kind = "missing"
                        if base: kind = "override" if age_seconds(base.get("time")) > stale_1m_sec else "available"
                        syn_1m[fl] = {"close": syn['close'], "time": syn['time'], "kind": kind}
                for itv in enabled_intervals:
                    cap = counts[itv]
                    s1 = st.session_state.series[itv].get(l1, [])
                    s2 = st.session_state.series[itv].get(l2, [])
                    s3 = st.session_state.series[itv].get(l3, [])
                    if s1 and s2 and s3:
                        syn_series[itv][fl] = weighted_series([s1,s2,s3], [1,-2,1], cap)
    return syn_1m, syn_series

def build_inter_synthetic_caches(inter_defs, pair_tol, span_tol, max_leg_age, enabled_intervals, counts):
    syn_1m = {}
    syn_series = {k:{} for k in enabled_intervals}
    for (a,b), dd in inter_defs.items():
        for io in dd['outr']:
            lA, lB = dd['legs_out'][io]
            rA = st.session_state.prices_1m.get(lA)
            rB = st.session_state.prices_1m.get(lB)
            if rA and rB and rA.get("close") is not None and rB.get("close") is not None:
                syn = weighted_latest([rA,rB], [1,-1], pair_tol, span_tol, max_leg_age)
                if syn:
                    syn_1m[io] = {"close": syn['close'], "time": syn['time'], "kind": "inter"}
            for itv in enabled_intervals:
                cap = counts[itv]
                sA = st.session_state.series[itv].get(lA, [])
                sB = st.session_state.series[itv].get(lB, [])
                if sA and sB:
                    syn_series[itv][io] = weighted_series([sA,sB], [1,-1], cap)
        for t, s_list in dd['spr'].items():
            for s in s_list:
                i1, i2 = dd['legs_spr'][s]
                r1 = syn_1m.get(i1); r2 = syn_1m.get(i2)
                if r1 and r2:
                    syn = weighted_latest([r1,r2], [1,-1], pair_tol, span_tol, max_leg_age)
                    if syn:
                        syn_1m[s] = {"close": syn['close'], "time": syn['time'], "kind": "inter"}
                for itv in enabled_intervals:
                    cap = counts[itv]
                    sI1 = syn_series[itv].get(i1, [])
                    sI2 = syn_series[itv].get(i2, [])
                    if sI1 and sI2:
                        syn_series[itv][s] = weighted_series([sI1,sI2], [1,-1], cap)
        for t, f_list in dd['fly'].items():
            for f in f_list:
                i1, i2, i3 = dd['legs_fly'][f]
                r1 = syn_1m.get(i1); r2 = syn_1m.get(i2); r3 = syn_1m.get(i3)
                if r1 and r2 and r3:
                    syn = weighted_latest([r1,r2,r3], [1,-2,1], pair_tol, span_tol, max_leg_age)
                    if syn:
                        syn_1m[f] = {"close": syn['close'], "time": syn['time'], "kind": "inter"}
                for itv in enabled_intervals:
                    cap = counts[itv]
                    sI1 = syn_series[itv].get(i1, [])
                    sI2 = syn_series[itv].get(i2, [])
                    sI3 = syn_series[itv].get(i3, [])
                    if sI1 and sI2 and sI3:
                        syn_series[itv][f] = weighted_series([sI1,sI2,sI3], [1,-2,1], cap)
    return syn_1m, syn_series

syn1m_intra, synseries_intra = build_intra_synthetic_caches(
    contracts_by_product, pair_tol, span_tol, max_leg_age, stale_1m,
    enabled_series_keys, series_counts
)
syn1m_inter, synseries_inter = build_inter_synthetic_caches(
    inter_defs, pair_tol, span_tol, max_leg_age, enabled_series_keys, series_counts
)

st.session_state.syn_1m = {**syn1m_intra, **syn1m_inter}
for k in enabled_series_keys:
    st.session_state.syn_series[k] = {**synseries_intra.get(k, {}), **synseries_inter.get(k, {})}

# =============================================================================
# 16) Reconcile & trusted sets
# =============================================================================
try:
    manual_overrides = json.loads(cfg.get("scale_overrides","{}"))
except Exception:
    manual_overrides = {}

def key_for_scale(sym: str, interval: str, strategy: str):
    return f"{sym}|{interval}|{strategy}"

trusted_api_1m = set()
trusted_syn_1m = set()
api_norm_factor_1m = {}

for sym in sorted_calendar(list(set(list(st.session_state.prices_1m.keys()) + list(st.session_state.syn_1m.keys())))):
    api_rec = st.session_state.prices_1m.get(sym)
    syn_rec = st.session_state.syn_1m.get(sym)
    strat = strategy_type_for(sym)
    k = key_for_scale(sym, "1M", strat)
    factor = 1.0
    if k in manual_overrides:
        try: factor = float(manual_overrides[k])
        except Exception: factor = 1.0
    a_tr, s_tr = reconcile_latest_1m(api_rec, syn_rec, abs_thr, rel_thr, factor=factor)
    if api_rec and age_seconds(api_rec.get('time')) > stale_1m and syn_rec is not None:
        s_tr, a_tr = True, False
        st.session_state.syn_1m[sym]['kind'] = "override"
    if a_tr: trusted_api_1m.add(sym)
    if s_tr: trusted_syn_1m.add(sym)
    if a_tr and factor != 1.0: api_norm_factor_1m[sym] = factor

st.session_state.trusted_api_1m = set(trusted_api_1m)
st.session_state.trusted_syn_1m = set(trusted_syn_1m)

trusted_api_series = {k:set() for k in enabled_series_keys}
trusted_syn_series = {k:set() for k in enabled_series_keys}
api_norm_factor_series = {k:{} for k in enabled_series_keys}

for itv in enabled_series_keys:
    api_map = st.session_state.series[itv]
    syn_map = st.session_state.syn_series[itv]
    syms = sorted_calendar(list(set(list(api_map.keys()) + list(syn_map.keys()))))
    for sym in syms:
        try:
            strat = strategy_type_for(sym)
            k = key_for_scale(sym, itv, strat)
            factor = 1.0
            a_bars = api_map.get(sym, [])
            s_bars = syn_map.get(sym, [])
            mape, ov = np.inf, 0
            accepted, reason = False, "NO_BOTH"
            if k in manual_overrides:
                try: factor = float(manual_overrides[k])
                except Exception: factor = 1.0
                accepted = True; reason = "MANUAL"
            elif a_bars and s_bars:
                da = {int(b['time']): float(b['close']) for b in a_bars}
                ds = {int(b['time']): float(b['close']) for b in s_bars}
                overlap = sorted(set(da.keys()) & set(ds.keys()))
                if overlap:
                    api_vals = [da[t] for t in overlap]
                    syn_vals = [ds[t] for t in overlap]
                    factor, mape, ov, accepted, reason = choose_scale_factor(api_vals, syn_vals, tol=cfg.get("mape_tol",0.5), min_overlap=cfg.get("min_overlap",5))
                else:
                    reason = "NO_OVERLAP"
            a_tr, s_tr = reconcile_series(a_bars, s_bars, abs_thr, rel_thr, factor=factor)
            if a_tr: trusted_api_series[itv].add(sym)
            if s_tr: trusted_syn_series[itv].add(sym)
            if a_tr and factor != 1.0:
                api_norm_factor_series[itv][sym] = factor
            st.session_state.audit_scale.append({
                "symbol": sym, "interval": itv, "strategy": strat, "factor": factor,
                "overlap_count": int(ov), "mape": float(mape) if np.isfinite(mape) else None,
                "accepted": bool(accepted), "reason": reason, "run_ts": utc_ms_now()
            })
        except Exception:
            continue

st.session_state.trusted_api_series = trusted_api_series
st.session_state.trusted_syn_series = trusted_syn_series
st.session_state.api_norm_factor_series = api_norm_factor_series
st.session_state.api_norm_factor_1m = api_norm_factor_1m

# =============================================================================
# 17) DB persistence helpers (IDs, latest tables, freshness, parquet)
# =============================================================================
def _meta_for(sym):
    return st.session_state.symbol_meta.get(sym, {
        "market_or_pair": None, "is_inter": None, "strategy": None, "tenor_months": None,
        "contract_tag": None, "base_product": None, "curve_bucket": None,
        "left_leg": None, "mid_leg": None, "right_leg": None
    })

def _star_ids_upsert(con):
    syms = [{"id": stable_u63("symbol:"+s), "symbol": s} for s in st.session_state.symbol_meta.keys()]
    if syms:
        _duckdb_upsert_df(con, "dim_symbol", pd.DataFrame(syms), ["id"])
    strategies = [{"id": stable_u63("strategy:"+x), "strategy": x} for x in ["outright","spread","fly","inter_outright","inter_spread","inter_fly"]]
    _duckdb_upsert_df(con, "dim_strategy", pd.DataFrame(strategies), ["id"])
    sources = [{"id": stable_u63("source:api"), "calc_method": "api"}, {"id": stable_u63("source:syn"), "calc_method": "syn"}]
    _duckdb_upsert_df(con, "dim_source", pd.DataFrame(sources), ["id"])
    markets = [{"id": stable_u63("market:"+m), "market_or_pair": m} for m in set([v["market_or_pair"] for v in st.session_state.symbol_meta.values() if v["market_or_pair"]])]
    if markets:
        _duckdb_upsert_df(con, "dim_market", pd.DataFrame(markets), ["id"])
    # Backfill IDs
    con.execute("""
        UPDATE timeseries t SET symbol_id = d.id
        FROM dim_symbol d
        WHERE t.symbol = d.symbol AND t.symbol_id IS NULL;
    """)
    con.execute("""
        UPDATE timeseries t SET market_or_pair_id = m.id
        FROM dim_market m
        WHERE t.market_or_pair = m.market_or_pair AND t.market_or_pair_id IS NULL;
    """)
    con.execute("""
        UPDATE timeseries t SET strategy_id = s.id
        FROM dim_strategy s
        WHERE t.strategy = s.strategy AND t.strategy_id IS NULL;
    """)
    con.execute("""
        UPDATE indicators i SET symbol_id = d.id
        FROM dim_symbol d
        WHERE i.symbol = d.symbol AND i.symbol_id IS NULL;
    """)
    con.execute("""
        UPDATE indicators i SET market_or_pair_id = m.id
        FROM dim_market m
        WHERE i.market_or_pair = m.market_or_pair AND i.market_or_pair_id IS NULL;
    """)
    con.execute("""
        UPDATE indicators i SET strategy_id = s.id
        FROM dim_strategy s
        WHERE i.strategy = s.strategy AND i.strategy_id IS NULL;
    """)

def _rebuild_materialized_latest(con):
    for itv in ["1M","1D","1H","5M"]:
        for src in ["api","syn"]:
            con.execute(f"""
                CREATE OR REPLACE TABLE latest_{itv}_{src} AS
                SELECT t.*
                FROM timeseries t
                JOIN (
                    SELECT symbol, MAX("time") AS mx
                    FROM timeseries
                    WHERE "interval"='{itv}' AND calc_method='{src}'
                    GROUP BY symbol
                ) m
                ON t.symbol = m.symbol AND t."time" = m.mx
                WHERE t."interval"='{itv}' AND t.calc_method='{src}';
            """)

def _update_freshness_sla(con, threshold_seconds: int):
    now_iso = datetime.utcnow().isoformat()
    for itv in ["1M","1D","1H","5M"]:
        for src in ["api","syn"]:
            df = con.execute(f"""
                SELECT symbol, '{itv}' AS interval, '{src}' AS calc_method, MAX("time") AS last_time
                FROM timeseries
                WHERE "interval"='{itv}' AND calc_method='{src}'
                GROUP BY symbol
            """).df()
            if df is None or df.empty: 
                continue
            df["age_seconds"] = (utc_ms_now() - df["last_time"].astype(np.int64)) / 1000.0
            status = []
            for a in df["age_seconds"]:
                if a <= threshold_seconds: status.append("fresh")
                elif a <= threshold_seconds*4: status.append("warm")
                else: status.append("stale")
            df["status"] = status
            df["threshold_seconds"] = int(threshold_seconds)
            df["updated_at_utc"] = now_iso
            df.rename(columns={"interval":"\"interval\""}, inplace=True)
            _duckdb_upsert_df(con, "freshness_sla", df.rename(columns={"\"interval\"":"interval"}), ["symbol","interval","calc_method"])

def _persist_scale_lookup(con):
    rows = []
    for sym, f in st.session_state.api_norm_factor_1m.items():
        rows.append({"symbol": sym, "interval": "1M", "calc_method": "api", "factor": float(f), "updated_at_utc": datetime.utcnow().isoformat()})
    for itv, mp in st.session_state.api_norm_factor_series.items():
        for sym, f in mp.items():
            rows.append({"symbol": sym, "interval": itv, "calc_method": "api", "factor": float(f), "updated_at_utc": datetime.utcnow().isoformat()})
    if rows:
        _duckdb_upsert_df(con, "scale_lookup", pd.DataFrame(rows), ["symbol","interval","calc_method"])

def _analyze(con):
    try: con.execute("PRAGMA analyze;")
    except Exception: pass

def _export_parquet_mirror(con):
    """Windows-safe Parquet export: short partition columns (mkt, cm, itv) + proper timestamp column."""
    if not cfg.get("export_parquet", True): return
    root = cfg.get("parquet_root", os.path.join(cfg.get("store_root"), "parquet_mirror"))
    ensure_dir(root)
    compression = cfg.get("parquet_compression","ZSTD")
    mode = cfg.get("parquet_mode","overwrite_current")
    subdir = "current" if mode == "overwrite_current" else datetime.utcnow().strftime("snap_%Y%m%d_%H%M%S")

    # OnlyTimeseries
    out_ts = os.path.join(root, subdir, "onlytimeseries")
    ensure_dir(out_ts)
    con.execute(f"""
        COPY (
            SELECT
               symbol, symbol_id, "interval", "time",
               to_timestamp("time"/1000.0) AS asof_utc,
               open, high, low, close, COALESCE(volume,0.0) AS volume,
               normalized_api_factor,
               market_or_pair, market_or_pair_id, is_inter, strategy, strategy_id, tenor_months, contract_tag, curve_bucket,
               calc_method, 'onlytimeseries'::TEXT AS dataset_kind,
               -- short partition aliases
               market_or_pair AS mkt, calc_method AS cm, "interval" AS itv
            FROM timeseries
        ) TO '{out_ts}'
        (FORMAT PARQUET, PARTITION_BY (mkt, cm, itv), COMPRESSION '{compression}', OVERWRITE_OR_IGNORE TRUE);
    """)

    # WithIndicators — join inline (ensures asof_utc and consistent schema)
    out_ind = os.path.join(root, subdir, "withindicators")
    ensure_dir(out_ind)
    con.execute(f"""
        COPY (
            SELECT
                t.symbol, t.symbol_id, t."interval", t."time",
                to_timestamp(t."time"/1000.0) AS asof_utc,
                t.open, t.high, t.low, t.close, COALESCE(t.volume,0.0) AS volume,
                t.normalized_api_factor,
                t.market_or_pair, t.market_or_pair_id, t.is_inter, t.strategy, t.strategy_id, t.tenor_months,
                t.contract_tag, t.curve_bucket,
                t.calc_method, 'withindicators'::TEXT AS dataset_kind,
                -- partition aliases
                t.market_or_pair AS mkt, t.calc_method AS cm, t."interval" AS itv,
                i.* EXCLUDE (symbol, symbol_id, "interval", "time", source, market_or_pair, market_or_pair_id, is_inter,
                              strategy, strategy_id, tenor_months, contract_tag, curve_bucket, calc_method, dataset_kind)
            FROM timeseries t
            LEFT JOIN indicators i
              ON t.symbol = i.symbol AND t."interval" = i."interval" AND t."time" = i."time" AND t.calc_method = i.calc_method
        ) TO '{out_ind}'
        (FORMAT PARQUET, PARTITION_BY (mkt, cm, itv), COMPRESSION '{compression}', OVERWRITE_OR_IGNORE TRUE);
    """)

def _maybe_snapshot_db():
    if not cfg.get("snapshot_on_save", True): return
    root = cfg.get("snapshot_root", os.path.join(cfg.get("store_root"), "db_snapshots"))
    ensure_dir(root)
    min_gap = int(cfg.get("snapshot_minutes", 30)) * 60
    now = time.time()
    if (now - st.session_state.last_snapshot_ts) < min_gap:
        return
    src = _duckdb_path()
    if not os.path.exists(src): return
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(root, f"market_data_{ts}.duckdb")
    try:
        shutil.copy2(src, dst)
        st.session_state.last_snapshot_ts = now
    except Exception as e:
        st.warning(f"Snapshot failed: {e}")

# =============================================================================
# 18) Indicator schema planner & eager precompute on Apply
# =============================================================================
def _all_families_union_from_cfg(cfg: dict) -> set:
    fams = set()
    gi = cfg.get("global_ind", {})
    if gi.get("enabled", False):
        f, _ = _normalize_family_selection(gi.get("families", []))
        fams.update(f or [])
    for code, pcfg in cfg.get("per_product", {}).items():
        ind = pcfg.get("indicators", {})
        for itv in ["1D","1H","5M"]:
            row = ind.get(itv, {})
            f, _ = _normalize_family_selection(row.get("selection", []))
            fams.update(f or [])
    for pair, icfg in cfg.get("intermarket", {}).items():
        for itv in ["1D","1H","5M"]:
            row = icfg.get(itv, {})
            f, _ = _normalize_family_selection(row.get("selection", []))
            fams.update(f or [])
    return fams

def _desired_indicator_columns_from_cfg(cfg: dict) -> set:
    fams = _all_families_union_from_cfg(cfg)
    if not fams:
        return set()
    n = 300
    t0 = utc_ms_now() - n*86400000
    bars = [{'time': t0 + i*86400000, 'open': 100+i*0.1, 'high': 100+i*0.2, 'low': 100+i*0.05, 'close':100+i*0.15, 'volume': 1000+i} for i in range(n)]
    df = compute_individual_indicators(bars, fams, cfg.get("indicator_sets", DEFAULT_INDICATOR_SETS), scope="full")
    base = {"time","Date","open","high","low","close","volume"}
    return set(c for c in df.columns if c not in base)

def _reset_indicator_schema(con) -> None:
    """Drop obsolete indicator columns so DB schema exactly matches current selection."""
    desired = _desired_indicator_columns_from_cfg(cfg)
    existing = _duckdb_table_columns(con, "indicators")
    base_cols = {"symbol","symbol_id","interval","time","source","market_or_pair","market_or_pair_id",
                 "is_inter","strategy","strategy_id","tenor_months","contract_tag","curve_bucket",
                 "calc_method","dataset_kind"}
    existing_ind_cols = set(c for c in existing if c not in base_cols)
    to_drop = sorted(list(existing_ind_cols - desired))
    for col in to_drop:
        try:
            con.execute(f'ALTER TABLE indicators DROP COLUMN "{col}";')
        except Exception:
            pass

def _families_for_symbol(sym: str, interval: str):
    fams = set(); scope = "full"
    gi = cfg.get("global_ind", {"enabled": False})
    if gi.get("enabled", False):
        fams_sel, _ = _normalize_family_selection(gi.get("families", ["ALL"]))
        fams = fams_sel; scope = gi.get("scope","full")
    if "-" in sym and len(sym.split("-")[0]) <= 3:
        pair_found = None
        for p in cfg.get("pairs", []):
            if sym.startswith(p):
                pair_found = p; break
        if pair_found:
            ic = cfg.get("intermarket", {}).get(pair_found, {}).get(interval, {})
            fams2, _ = _normalize_family_selection(ic.get("selection", []))
            if fams2: fams = fams2; scope = ic.get("scope","full")
    else:
        base = sym[:3]
        ic = cfg.get("per_product", {}).get(base, {}).get("indicators", {}).get(interval, {})
        fams2, _ = _normalize_family_selection(ic.get("selection", []))
        if fams2: fams = fams2; scope = ic.get("scope","full")
    return fams, scope

@st.cache_data(ttl=300, show_spinner=False)
def db_fetch_timeseries(symbol: str, interval: str, source: str) -> list:
    con = _duckdb_connect()
    if con is None: return []
    try:
        _duckdb_init(con)
        q = """
            SELECT "time", open, high, low, close, COALESCE(volume, 0.0) AS volume
            FROM timeseries
            WHERE symbol = ? AND "interval" = ? AND calc_method = ?
            ORDER BY "time" ASC
        """
        df = con.execute(q, [symbol, interval, source]).df()
        if df.empty:
            return []
        df['time'] = df['time'].astype(np.int64)
        return df.to_dict(orient="records")
    except Exception:
        return []
    finally:
        try: con.close()
        except Exception: pass

def db_upsert_indicators_df(symbol: str, interval: str, source: str, df: pd.DataFrame):
    if df is None or df.empty: return
    df2 = df.copy()
    if "Date" in df2.columns:
        df2.drop(columns=["Date"], inplace=True)
    meta = _meta_for(symbol)
    df2["symbol"] = symbol
    df2["interval"] = interval
    df2["source"] = source
    df2["market_or_pair"] = meta["market_or_pair"]
    df2["is_inter"] = meta["is_inter"]
    df2["strategy"] = meta["strategy"]
    df2["tenor_months"] = meta["tenor_months"]
    df2["contract_tag"] = meta["contract_tag"]
    df2["curve_bucket"] = meta["curve_bucket"]
    df2["calc_method"] = source
    df2["dataset_kind"] = "withindicators"
    if "time" in df2.columns:
        df2["time"] = df2["time"].astype(np.int64)
    con = _duckdb_connect()
    if con is None: return
    try:
        _duckdb_init(con)
        _duckdb_upsert_df(con, "indicators", df2, ["symbol","interval","calc_method","time"])
    finally:
        try: con.close()
        except Exception: pass

def _indicator_warmup_len(cfg: dict) -> int:
    s = cfg.get("indicator_sets", DEFAULT_INDICATOR_SETS)
    wins = []
    wins += s.get("EMA", []) or []
    wins += s.get("ATR", []) or []
    wins += s.get("RSI", []) or []
    wins += s.get("ADX", []) or []
    wins += s.get("SMA", []) or []
    wins += s.get("Ranges", []) or []
    wins += s.get("Range/ATR", {}).get("ranges", []) or []
    wins += s.get("Range/ATR", {}).get("atrs", []) or []
    wins += s.get("Range/Range", {}).get("numerators", []) or []
    wins += s.get("Range/Range", {}).get("denominators", []) or []
    wins += s.get("BB", {}).get("periods", []) or []
    wins += s.get("KC", {}).get("periods", []) or []
    wins += s.get("KC", {}).get("atr_periods", []) or []
    wins += s.get("SuperTrend", {}).get("atr_periods", []) or []
    wins += s.get("MACD", {}).get("fast", []) or []
    wins += s.get("MACD", {}).get("slow", []) or []
    wins += s.get("MACD", {}).get("signal", []) or []
    wins += s.get("Aroon", []) or []
    wins += s.get("CCI", []) or []
    wins += s.get("ZPRICE", []) or []
    wins += s.get("ZRET", []) or []
    M = max([int(x) for x in wins] + [50])
    return max(600, 5*M)

def _eager_precompute_indicators():
    """Drop obsolete indicator columns, then compute & upsert indicators for all active slices."""
    con = _duckdb_connect()
    if con is None: return
    try:
        _duckdb_init(con)
        _reset_indicator_schema(con)  # drop old columns first
        df_active = con.execute("""
            SELECT DISTINCT symbol, "interval", calc_method
            FROM timeseries
            WHERE "interval" IN ('1D','1H','5M')
        """).df()
    finally:
        try: con.close()
        except Exception: pass
    if df_active is None or df_active.empty:
        return
    warmup = _indicator_warmup_len(cfg)
    for _, row in df_active.iterrows():
        sym = row["symbol"]; itv = row["interval"]; src = row["calc_method"]
        fams, scope = _families_for_symbol(sym, itv)
        if not fams:
            continue
        bars = db_fetch_timeseries(sym, itv, src)
        if not bars:
            continue
        if len(bars) > warmup:
            bars = bars[-warmup:]
        df_ind = compute_individual_indicators(bars, fams, cfg.get("indicator_sets", DEFAULT_INDICATOR_SETS), scope)
        if df_ind is None or df_ind.empty:
            continue
        db_upsert_indicators_df(sym, itv, src, df_ind)

# =============================================================================
# 19) Persist timeseries to DB + eager indicators + views + parquet
# =============================================================================
def _persist_timeseries_to_db():
    if not cfg.get("autosave", True): return
    con = _duckdb_connect()
    if con is None: return
    try:
        _duckdb_init(con)
        # Contracts catalog
        cat_rows = []
        for sym, meta in st.session_state.symbol_meta.items():
            cat_rows.append({
                "symbol": sym, "base_product": meta.get("base_product"),
                "market_or_pair": meta.get("market_or_pair"),
                "is_inter": meta.get("is_inter"),
                "strategy": meta.get("strategy"),
                "tenor_months": meta.get("tenor_months"),
                "contract_tag": meta.get("contract_tag"),
                "curve_bucket": meta.get("curve_bucket"),
                "left_leg": meta.get("left_leg"), "mid_leg": meta.get("mid_leg"), "right_leg": meta.get("right_leg")
            })
        if cat_rows:
            _duckdb_upsert_df(con, "contracts_catalog", pd.DataFrame(cat_rows), ["symbol"])

        # 1M
        rows_1m = []
        for sym, rec in st.session_state.prices_1m.items():
            meta = _meta_for(sym)
            rows_1m.append({
                "symbol": sym, "interval": "1M", "time": int(rec["time"]),
                "open": float(rec["close"]), "high": float(rec["close"]), "low": float(rec["close"]), "close": float(rec["close"]),
                "volume": 0.0, "source": "api", "normalized_api_factor": float(st.session_state.api_norm_factor_1m.get(sym, 1.0)),
                "market_or_pair": meta["market_or_pair"], "is_inter": meta["is_inter"],
                "strategy": meta["strategy"], "tenor_months": meta["tenor_months"], "contract_tag": meta["contract_tag"],
                "curve_bucket": meta["curve_bucket"],
                "calc_method": "api", "dataset_kind": "onlytimeseries"
            })
        for sym, rec in st.session_state.syn_1m.items():
            meta = _meta_for(sym)
            rows_1m.append({
                "symbol": sym, "interval": "1M", "time": int(rec["time"]),
                "open": float(rec["close"]), "high": float(rec["close"]), "low": float(rec["close"]), "close": float(rec["close"]),
                "volume": 0.0, "source": "syn", "normalized_api_factor": None,
                "market_or_pair": meta["market_or_pair"], "is_inter": meta["is_inter"],
                "strategy": meta["strategy"], "tenor_months": meta["tenor_months"], "contract_tag": meta["contract_tag"],
                "curve_bucket": meta["curve_bucket"],
                "calc_method": "syn", "dataset_kind": "onlytimeseries"
            })
        if rows_1m:
            _duckdb_upsert_df(con, "timeseries", pd.DataFrame(rows_1m), ["symbol","interval","calc_method","time"])

        # Series (API + SYN)
        for itv in enabled_series_keys:
            rows = []
            for sym, bars in st.session_state.series[itv].items():
                meta = _meta_for(sym)
                nf = float(st.session_state.api_norm_factor_series.get(itv, {}).get(sym, 1.0))
                for b in bars:
                    rows.append({
                        "symbol": sym, "interval": itv, "time": int(b["time"]),
                        "open": float(b["open"]), "high": float(b["high"]), "low": float(b["low"]), "close": float(b["close"]),
                        "volume": float(b.get("volume",0.0)), "source": "api", "normalized_api_factor": nf,
                        "market_or_pair": meta["market_or_pair"], "is_inter": meta["is_inter"],
                        "strategy": meta["strategy"], "tenor_months": meta["tenor_months"], "contract_tag": meta["contract_tag"],
                        "curve_bucket": meta["curve_bucket"],
                        "calc_method": "api", "dataset_kind": "onlytimeseries"
                    })
            for sym, bars in st.session_state.syn_series[itv].items():
                meta = _meta_for(sym)
                for b in bars:
                    rows.append({
                        "symbol": sym, "interval": itv, "time": int(b["time"]),
                        "open": float(b["open"]), "high": float(b["high"]), "low": float(b["low"]), "close": float(b["close"]),
                        "volume": float(b.get("volume",0.0)), "source": "syn", "normalized_api_factor": None,
                        "market_or_pair": meta["market_or_pair"], "is_inter": meta["is_inter"],
                        "strategy": meta["strategy"], "tenor_months": meta["tenor_months"], "contract_tag": meta["contract_tag"],
                        "curve_bucket": meta["curve_bucket"],
                        "calc_method": "syn", "dataset_kind": "onlytimeseries"
                    })
            if rows:
                _duckdb_upsert_df(con, "timeseries", pd.DataFrame(rows), ["symbol","interval","calc_method","time"])

        # Loaded contracts
        lc_rows = []
        for code, defs in contracts_by_product.items():
            for sym in defs['outr']:
                meta = _meta_for(sym)
                lc_rows.append({"market_or_pair": code, "is_inter": False, "symbol": sym, "interval": "1M", "source": "api",
                                "strategy": "outright", "tenor_months": None, "contract_tag": meta["contract_tag"],
                                "curve_bucket": meta["curve_bucket"],
                                "left_leg": None, "mid_leg": None, "right_leg": None})
            for t, lst in defs['spr'].items():
                for sym in lst:
                    parts = sym.split("-"); meta = _meta_for(sym)
                    lc_rows.append({"market_or_pair": code, "is_inter": False, "symbol": sym, "interval": "1M", "source": "api",
                                    "strategy": "spread", "tenor_months": int(t), "contract_tag": meta["contract_tag"],
                                    "curve_bucket": meta["curve_bucket"],
                                    "left_leg": parts[0], "mid_leg": None, "right_leg": f"{code}{parts[1]}"})
            for t, lst in defs['fly'].items():
                for sym in lst:
                    parts = sym.split("-"); meta = _meta_for(sym)
                    lc_rows.append({"market_or_pair": code, "is_inter": False, "symbol": sym, "interval": "1M", "source": "api",
                                    "strategy": "fly", "tenor_months": int(t), "contract_tag": meta["contract_tag"],
                                    "curve_bucket": meta["curve_bucket"],
                                    "left_leg": parts[0], "mid_leg": f"{code}{parts[1]}", "right_leg": f"{code}{parts[2]}"})
        for (a,b), defs in inter_defs.items():
            key = f"{a}-{b}"
            for sym in defs['outr']:
                l,r = defs['legs_out'][sym]; meta=_meta_for(sym)
                lc_rows.append({"market_or_pair": key, "is_inter": True, "symbol": sym, "interval": "1M", "source": "syn",
                                "strategy": "inter_outright", "tenor_months": None, "contract_tag": meta["contract_tag"],
                                "curve_bucket": meta["curve_bucket"],
                                "left_leg": l, "mid_leg": None, "right_leg": r})
            for t, lst in defs['spr'].items():
                for sym in lst:
                    i1,i2 = defs['legs_spr'][sym]; meta=_meta_for(sym)
                    lc_rows.append({"market_or_pair": key, "is_inter": True, "symbol": sym, "interval": "1M", "source": "syn",
                                    "strategy": "inter_spread", "tenor_months": int(t), "contract_tag": meta["contract_tag"],
                                    "curve_bucket": meta["curve_bucket"],
                                    "left_leg": i1, "mid_leg": None, "right_leg": i2})
            for t, lst in defs['fly'].items():
                for sym in lst:
                    i1,i2,i3 = defs['legs_fly'][sym]; meta=_meta_for(sym)
                    lc_rows.append({"market_or_pair": key, "is_inter": True, "symbol": sym, "interval": "1M", "source": "syn",
                                    "strategy": "inter_fly", "tenor_months": int(t), "contract_tag": meta["contract_tag"],
                                    "curve_bucket": meta["curve_bucket"],
                                    "left_leg": i1, "mid_leg": i2, "right_leg": i3})
        if lc_rows:
            df_lc = pd.DataFrame(lc_rows).drop_duplicates(subset=["market_or_pair","is_inter","symbol","interval","source"])
            _duckdb_upsert_df(con, "loaded_contracts", df_lc, ["market_or_pair","interval","source","symbol"])

        # Audit scales / lookup
        if st.session_state.audit_scale:
            df_a = pd.DataFrame(st.session_state.audit_scale)
            _duckdb_upsert_df(con, "scales_audit", df_a, ["symbol","interval","strategy","run_ts"])
        _persist_scale_lookup(con)

        # Star IDs backfill (with deterministic IDs)
        _star_ids_upsert(con)

        # Latest materialized
        _rebuild_materialized_latest(con)

        # Freshness SLA
        _update_freshness_sla(con, DATA_FRESHNESS_THRESHOLD_SECONDS)

        # Analyze
        _analyze(con)

        # Parquet mirror (Windows-safe)
        _export_parquet_mirror(con)

    finally:
        try: con.close()
        except Exception: pass

    # External DB snapshot
    _maybe_snapshot_db()

# Persist and then eagerly precompute indicators
_persist_timeseries_to_db()
_eager_precompute_indicators()

# =============================================================================
# 20) UI helpers (DB reads, charts, filters)
# =============================================================================
@st.cache_data(ttl=300, show_spinner=False)
def db_fetch_timeseries_full(symbol: str, interval: str, source: str) -> pd.DataFrame:
    con = _duckdb_connect()
    if con is None: return pd.DataFrame()
    try:
        _duckdb_init(con)
        q = """
            SELECT "time",
                   to_timestamp("time"/1000.0) AS asof_utc,
                   open, high, low, close, COALESCE(volume,0.0) AS volume,
                   normalized_api_factor, market_or_pair, strategy, tenor_months, contract_tag, curve_bucket,
                   calc_method
            FROM timeseries
            WHERE symbol = ? AND "interval" = ? AND calc_method = ?
            ORDER BY "time" ASC
        """
        df = con.execute(q, [symbol, interval, source]).df()
        if not df.empty:
            df["time"] = df["time"].astype(np.int64)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        try: con.close()
        except Exception: pass

@st.cache_data(ttl=600, show_spinner=False)
def db_fetch_indicators_df(symbol: str, interval: str, source: str) -> pd.DataFrame:
    con = _duckdb_connect()
    if con is None: return pd.DataFrame()
    try:
        _duckdb_init(con)
        q = """
            SELECT *
            FROM indicators
            WHERE symbol = ? AND "interval" = ? AND calc_method = ?
            ORDER BY "time" ASC
        """
        df = con.execute(q, [symbol, interval, source]).df()
        if df.empty: return df
        if "time" in df.columns: df["time"] = df["time"].astype(np.int64)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        try: con.close()
        except Exception: pass

def _prefer_db(): return bool(cfg.get("prefer_db_display", True))
def _hard_lock(): return bool(cfg.get("db_hard_lock", True))

def _auto_source_for(sym: str, interval: str):
    if _prefer_db():
        con = _duckdb_connect()
        if con:
            try:
                c_api = con.execute('SELECT COUNT(*) FROM timeseries WHERE symbol=? AND "interval"=? AND calc_method="api";', [sym, interval]).fetchone()[0]
                if c_api > 0: 
                    con.close(); return "db_api"
                c_syn = con.execute('SELECT COUNT(*) FROM timeseries WHERE symbol=? AND "interval"=? AND calc_method="syn";', [sym, interval]).fetchone()[0]
                con.close()
                if c_syn > 0: return "db_syn"
            except Exception:
                try: con.close()
                except Exception: pass
    if sym in st.session_state.trusted_api_series.get(interval, set()): return "api"
    if sym in st.session_state.trusted_syn_series.get(interval, set()): return "syn"
    return "api" if sym in st.session_state.series.get(interval, {}) else "syn"

def _get_series(sym: str, interval: str, mode: str):
    try:
        if mode == "api":    return st.session_state.series.get(interval, {}).get(sym, [])
        if mode == "syn":    return st.session_state.syn_series.get(interval, {}).get(sym, [])
        if mode == "db_api": return db_fetch_timeseries(sym, interval, "api")
        if mode == "db_syn": return db_fetch_timeseries(sym, interval, "syn")
    except Exception:
        return []
    return []

def _get_indicators_df(sym: str, interval: str, mode: str):
    try:
        if mode.startswith("db_"):
            src = "api" if mode.endswith("api") else "syn"
            return db_fetch_indicators_df(sym, interval, src)
        src = "api" if mode == "api" else "syn"
        df = db_fetch_indicators_df(sym, interval, src)
        if not df.empty:
            return df
        fams, scope = _families_for_symbol(sym, interval)
        if not fams:
            return pd.DataFrame()
        bars = _get_series(sym, interval, mode)
        if not bars:
            return pd.DataFrame()
        df_ind = compute_individual_indicators(bars, fams, cfg.get("indicator_sets", DEFAULT_INDICATOR_SETS), scope)
        if df_ind is None or df_ind.empty:
            return pd.DataFrame()
        db_upsert_indicators_df(sym, interval, src, df_ind)
        return df_ind
    except Exception:
        return pd.DataFrame()

def _status_ts_for_symbol(sym: str):
    con = _duckdb_connect()
    if con is None: return None
    try:
        _duckdb_init(con)
        t_api = con.execute('SELECT MAX("time") FROM timeseries WHERE symbol=? AND "interval"="1M" AND calc_method="api";', [sym]).fetchone()[0]
        t_syn = con.execute('SELECT MAX("time") FROM timeseries WHERE symbol=? AND "interval"="1M" AND calc_method="syn";', [sym]).fetchone()[0]
        latest = max([t for t in [t_api, t_syn] if t is not None], default=None)
        return int(latest) if latest is not None else None
    except Exception:
        return None
    finally:
        try: con.close()
        except Exception: pass

def _status_symbol(ts_ms: int):
    if ts_ms is None: return "⚪"
    age = (utc_ms_now() - int(ts_ms)) / 1000.0
    if age <= DATA_FRESHNESS_THRESHOLD_SECONDS: return "🟢"
    if age <= DATA_FRESHNESS_THRESHOLD_SECONDS*4: return "🟡"
    return "🔴"

def _value_fmt(x):
    try:
        if x is None: return "—"
        ax = abs(float(x))
        if ax == 0: return "0"
        if ax >= 1e9: return f"{x/1e9:.2f}B"
        if ax >= 1e6: return f"{x/1e6:.2f}M"
        if ax >= 1e3: return f"{x/1e3:.2f}k"
        if ax >= 1:   return f"{x:.4f}"
        return f"{x:.6f}"
    except Exception:
        return str(x)

def _chart_toolbar(sym: str, default_interval="1D"):
    with st.container():
        c1,c2,c3,c4 = st.columns([1.1,1.1,1.1,2.2])
        interval = c1.selectbox("Interval", ["1D","1H","5M"], index=["1D","1H","5M"].index(default_interval), key=f"{sym}_itv")
        src_default = cfg.get("default_chart_source","auto")
        if _hard_lock(): src_opts = ["Auto","DB API","DB SYN"]
        else:            src_opts = ["Auto","API","SYN","DB API","DB SYN"]
        src_box = c2.selectbox("Data", src_opts, index=max(0, src_opts.index({"auto":"Auto","api":"API","syn":"SYN","db_api":"DB API","db_syn":"DB SYN"}[src_default])) if {"auto":"Auto","api":"API","syn":"SYN","db_api":"DB API","db_syn":"DB SYN"}[src_default] in src_opts else 0, key=f"{sym}_src")
        add_ind = c3.checkbox("Indicators", value=True, key=f"{sym}_ind")
        range_sel = c4.selectbox("Range", ["All","2Y","1Y","6M","3M","1M","2W","1W"], index=0, key=f"{sym}_rng")
        mode = {"Auto":"auto","API":"api","SYN":"syn","DB API":"db_api","DB SYN":"db_syn"}[src_box]
        return interval, mode, add_ind, range_sel

def _apply_range(fig, df, choice):
    if df.empty: return fig
    t = df['time'].values
    if choice == "All": return fig
    now = int(t[-1])
    ms_day = 86400000
    map_days = {"2Y": 730, "1Y": 365, "6M": 182, "3M": 90, "1M": 30, "2W": 14, "1W": 7}
    if choice in map_days:
        start = now - map_days[choice]*ms_day
        fig.update_xaxes(range=[start, now])
    return fig

def _plot_symbol(sym: str, interval: str, mode: str, show_indicators: bool, range_choice: str):
    if mode == "auto":
        mode = _auto_source_for(sym, interval)
        if _hard_lock() and mode in ("api","syn"):
            mode = "db_syn"
    bars = _get_series(sym, interval, mode)
    if not bars:
        st.info("No data available for the selected source.", icon="ℹ️")
        return
    df = pd.DataFrame(bars)
    df['Date'] = pd.to_datetime(df['time'], unit='ms')

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df['Date'],
        open=df['open'], high=df['high'], low=df['low'], close=df['close'],
        name=f"{sym} {interval} ({mode.upper()})"
    ))

    if show_indicators:
        ind_df = _get_indicators_df(sym, interval, mode)
        for col in [c for c in ind_df.columns if any(x in c for x in [
            "EMA_","SMA_","BB_MID_","BB_pctB_","KC_MID_","RSI_","ATR","ADX_","Aroon","CCI_","ZPRICE_","ZRET_"])][:8]:
            if col in ("symbol","interval","time","source","open","high","low","close","volume","dataset_kind","calc_method","market_or_pair","strategy","tenor_months","contract_tag","is_inter","curve_bucket","symbol_id","market_or_pair_id","strategy_id","asof_utc","mkt","cm","itv"):
                continue
            try:
                x = pd.to_datetime(ind_df['time'], unit='ms')
                y = pd.to_numeric(ind_df[col], errors='coerce')
                fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=col, opacity=0.6))
            except Exception:
                continue

    fig.update_layout(height=450, margin=dict(l=10,r=10,t=30,b=30), xaxis_rangeslider_visible=False, template="plotly_dark")
    fig = _apply_range(fig, df[['time']], range_choice)
    st.plotly_chart(fig, use_container_width=True)

# ----- Global filters (namespaced keys to avoid duplicates) -----
def _global_filters_ui(prefix: str):
    st.markdown("<div class='sticky-controls'>", unsafe_allow_html=True)
    c1,c2,c3,c4,c5 = st.columns([1.2,1.2,1.2,1.2,1.2])
    with c1:
        strat = st.selectbox("Strategy", ["(any)","outright","spread","fly","inter_outright","inter_spread","inter_fly"], index=0, key=f"{prefix}_gf_strat")
    with c2:
        curve = st.selectbox("Curve", ["(any)","front","mid","back"], index=0, key=f"{prefix}_gf_curve")
    with c3:
        tenor = st.text_input("Tenor (m)", value="", key=f"{prefix}_gf_tenor")
    with c4:
        mkt = st.text_input("Market/Pair contains", value="", key=f"{prefix}_gf_mkt")
    with c5:
        fresh = st.selectbox("Freshness", ["(any)","fresh","warm","stale"], index=0, key=f"{prefix}_gf_fresh")
    st.markdown("</div>", unsafe_allow_html=True)
    return {"strat": strat, "curve": curve, "tenor": tenor, "mkt": mkt, "fresh": fresh}

def _passes_global_filters(sym: str, filters: dict):
    meta = _meta_for(sym)
    strat = filters.get("strat","(any)")
    curve = filters.get("curve","(any)")
    tenor = filters.get("tenor","")
    mkt   = filters.get("mkt","")
    fresh = filters.get("fresh","(any)")
    if strat != "(any)" and meta.get("strategy") != strat: return False
    if curve != "(any)" and (meta.get("curve_bucket") or "") != curve: return False
    if tenor.strip():
        try:
            if int(meta.get("tenor_months") or -1) != int(tenor.strip()): return False
        except Exception:
            return False
    if mkt.strip() and mkt.strip().lower() not in (meta.get("market_or_pair") or "").lower(): return False
    if fresh != "(any)":
        ts = _status_ts_for_symbol(sym)
        stat = _status_symbol(ts)
        if fresh == "fresh" and stat != "🟢": return False
        if fresh == "warm"  and stat != "🟡": return False
        if fresh == "stale" and stat != "🔴": return False
    return True

def indicator_name_list(sym: str, interval: str) -> str:
    cols = []
    for src in ["api","syn"]:
        df = db_fetch_indicators_df(sym, interval, src)
        if df.empty:
            fams, scope = _families_for_symbol(sym, interval)
            bars = db_fetch_timeseries(sym, interval, src)
            if not fams or not bars:
                continue
            try:
                df = compute_individual_indicators(bars[-200:], fams, cfg.get("indicator_sets", DEFAULT_INDICATOR_SETS), "latest")
            except Exception:
                df = pd.DataFrame()
        if not df.empty:
            base = {"symbol","interval","time","source","open","high","low","close","volume","dataset_kind","calc_method",
                    "market_or_pair","strategy","tenor_months","contract_tag","is_inter","curve_bucket","symbol_id","market_or_pair_id","strategy_id","asof_utc","mkt","cm","itv"}
            cols_src = [c for c in df.columns if c not in base]
            cols.extend(cols_src)
    cols = sorted(set(cols))
    return ", ".join(cols[:25]) + (" …" if len(cols)>25 else "")

def _build_editor_table(symbols, filters: dict):
    rows = []
    for sym in symbols:
        if not _passes_global_filters(sym, filters):
            continue
        try:
            meta = _meta_for(sym)
            # latest 1M (DB)
            con = _duckdb_connect()
            ts_api = ts_syn = None; api_1m_val = syn_1m_val = None
            if con:
                try:
                    row_api = con.execute('SELECT close,"time" FROM latest_1M_api WHERE symbol=?;', [sym]).fetchone()
                    row_syn = con.execute('SELECT close,"time" FROM latest_1M_syn WHERE symbol=?;', [sym]).fetchone()
                    if row_api: api_1m_val, ts_api = float(row_api[0]), int(row_api[1])
                    if row_syn: syn_1m_val, ts_syn = float(row_syn[0]), int(row_syn[1])
                except Exception:
                    pass
                try: con.close()
                except Exception: pass
            stsym = _status_symbol(max([t for t in [ts_api, ts_syn] if t is not None], default=None))
            row = {
                "Status": stsym,
                "Symbol": sym,
                "Type": meta.get("contract_tag"),
                "Curve": meta.get("curve_bucket") or "",
                "Market/Pair": meta.get("market_or_pair"),
                "Tenor (m)": meta.get("tenor_months") or "",
                "1M API": _value_fmt(api_1m_val) if api_1m_val is not None else "—",
                "1M API Age": human_age(ts_api) if ts_api else "N/A",
                "1M SYN": _value_fmt(syn_1m_val) if syn_1m_val is not None else "—",
                "1M SYN Age": human_age(ts_syn) if ts_syn else "N/A",
                # 1D
                "1D API cnt": len(db_fetch_timeseries(sym, "1D", "api")),
                "1D SYN cnt": len(db_fetch_timeseries(sym, "1D", "syn")),
                "1D API range": "—",
                "1D SYN range": "—",
                "1D Indicators": indicator_name_list(sym, "1D"),
                "1D API Table": False,
                "1D SYN Table": False,
                "1D API+Ind": False,
                "1D SYN+Ind": False,
                # 1H
                "1H API cnt": len(db_fetch_timeseries(sym, "1H", "api")),
                "1H SYN cnt": len(db_fetch_timeseries(sym, "1H", "syn")),
                "1H API range": "—",
                "1H SYN range": "—",
                "1H Indicators": indicator_name_list(sym, "1H"),
                "1H API Table": False,
                "1H SYN Table": False,
                "1H API+Ind": False,
                "1H SYN+Ind": False,
                # 5M
                "5M API cnt": len(db_fetch_timeseries(sym, "5M", "api")),
                "5M SYN cnt": len(db_fetch_timeseries(sym, "5M", "syn")),
                "5M API range": "—",
                "5M SYN range": "—",
                "5M Indicators": indicator_name_list(sym, "5M"),
                "5M API Table": False,
                "5M SYN Table": False,
                "5M API+Ind": False,
                "5M SYN+Ind": False,
                "Chart": False
            }
            rows.append(row)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.sort_values(by=["Symbol"], key=lambda s: s.map(calendar_key))
    return df

def _render_actions_from_editor(df_actions: pd.DataFrame, default_interval="1D"):
    if df_actions is None or df_actions.empty:
        return
    for _, row in df_actions.iterrows():
        sym = row["Symbol"]
        def _table(sym, itv, src, with_ind=False):
            src_key = "api" if src.upper()=="API" else "syn"
            df_full = db_fetch_timeseries_full(sym, itv, src_key)
            if df_full.empty:
                st.info(f"{sym} · {itv} · {src}: no data.", icon="ℹ️")
                return
            df_full["scaled_close"] = df_full["close"] * df_full["normalized_api_factor"].fillna(1.0)
            df_full["recon_abs_thr"] = cfg.get("abs_thr", DEFAULT_ABS_RECON_THR)
            df_full["recon_rel_thr"] = cfg.get("rel_thr", DEFAULT_REL_RECON_THR)
            st.markdown(f"**{sym} · {itv} · {src}**")
            base_cols = ["time","asof_utc","open","high","low","close","volume","scaled_close","normalized_api_factor","recon_abs_thr","recon_rel_thr"]
            part_cols = ["market_or_pair","strategy","tenor_months","contract_tag","curve_bucket","calc_method"]
            disp = df_full[base_cols+part_cols].copy()
            disp["Date"] = pd.to_datetime(disp["time"], unit="ms")
            disp = disp[["Date"] + [c for c in disp.columns if c!="Date"]]
            st.dataframe(disp.tail(500), use_container_width=True, height=260)
            if with_ind:
                df_ind = db_fetch_indicators_df(sym, itv, src_key)
                if df_ind.empty:
                    fams, scope = _families_for_symbol(sym, itv)
                    bars = db_fetch_timeseries(sym, itv, src_key)
                    if bars and fams:
                        df_ind = compute_individual_indicators(bars, fams, cfg.get("indicator_sets", DEFAULT_INDICATOR_SETS), scope)
                        if not df_ind.empty:
                            db_upsert_indicators_df(sym, itv, src_key, df_ind)
                if not df_ind.empty:
                    st.caption("With indicators")
                    join = pd.merge(df_full[["time"]+part_cols], df_ind, on="time", how="right")
                    join["Date"] = pd.to_datetime(join["time"], unit="ms")
                    st.dataframe(join.tail(300), use_container_width=True, height=300)

        # 1D
        if bool(row.get("1D API Table", False)):   _table(sym, "1D", "API", with_ind=False)
        if bool(row.get("1D SYN Table", False)):   _table(sym, "1D", "SYN", with_ind=False)
        if bool(row.get("1D API+Ind", False)):     _table(sym, "1D", "API", with_ind=True)
        if bool(row.get("1D SYN+Ind", False)):     _table(sym, "1D", "SYN", with_ind=True)
        # 1H
        if bool(row.get("1H API Table", False)):   _table(sym, "1H", "API", with_ind=False)
        if bool(row.get("1H SYN Table", False)):   _table(sym, "1H", "SYN", with_ind=False)
        if bool(row.get("1H API+Ind", False)):     _table(sym, "1H", "API", with_ind=True)
        if bool(row.get("1H SYN+Ind", False)):     _table(sym, "1H", "SYN", with_ind=True)
        # 5M
        if bool(row.get("5M API Table", False)):   _table(sym, "5M", "API", with_ind=False)
        if bool(row.get("5M SYN Table", False)):   _table(sym, "5M", "SYN", with_ind=False)
        if bool(row.get("5M API+Ind", False)):     _table(sym, "5M", "API", with_ind=True)
        if bool(row.get("5M SYN+Ind", False)):     _table(sym, "5M", "SYN", with_ind=True)

        if bool(row.get("Chart", False)):
            st.markdown("---")
            st.markdown(f"### {sym}  <span class='pill'>{_meta_for(sym).get('contract_tag')}</span> <span class='pill'>{_meta_for(sym).get('market_or_pair')}</span>", unsafe_allow_html=True)
            interval, mode, add_ind, rng = _chart_toolbar(sym, default_interval=default_interval)
            _plot_symbol(sym, interval, mode, add_ind, rng)

def _editor_column_config():
    from streamlit import column_config as cc
    cfg_map = {
        "Status": cc.TextColumn("Status"),
        "Symbol": cc.TextColumn("Contracts"),
        "Type": cc.TextColumn("Tag"),
        "Curve": cc.TextColumn("Curve"),
        "Market/Pair": cc.TextColumn("Market/Pair"),
        "Tenor (m)": cc.NumberColumn("Tenor (m)"),
        "1M API": cc.TextColumn("1M API Close"),
        "1M API Age": cc.TextColumn("1M API Age"),
        "1M SYN": cc.TextColumn("1M Synthetic Close"),
        "1M SYN Age": cc.TextColumn("1M SYN Age"),
        # 1D
        "1D API cnt": cc.NumberColumn("1D API cnt"),
        "1D SYN cnt": cc.NumberColumn("1D SYN cnt"),
        "1D API range": cc.TextColumn("1D API Range"),
        "1D SYN range": cc.TextColumn("1D SYN Range"),
        "1D Indicators": cc.TextColumn("1D Indicators"),
        "1D API Table": cc.CheckboxColumn("1D API Data Table"),
        "1D SYN Table": cc.CheckboxColumn("1D SYN Data Table"),
        "1D API+Ind": cc.CheckboxColumn("1D API + Indicators"),
        "1D SYN+Ind": cc.CheckboxColumn("1D SYN + Indicators"),
        # 1H
        "1H API cnt": cc.NumberColumn("1H API cnt"),
        "1H SYN cnt": cc.NumberColumn("1H SYN cnt"),
        "1H API range": cc.TextColumn("1H API Range"),
        "1H SYN range": cc.TextColumn("1H SYN Range"),
        "1H Indicators": cc.TextColumn("1H Indicators"),
        "1H API Table": cc.CheckboxColumn("1H API Data Table"),
        "1H SYN Table": cc.CheckboxColumn("1H SYN Data Table"),
        "1H API+Ind": cc.CheckboxColumn("1H API + Indicators"),
        "1H SYN+Ind": cc.CheckboxColumn("1H SYN + Indicators"),
        # 5M
        "5M API cnt": cc.NumberColumn("5M API cnt"),
        "5M SYN cnt": cc.NumberColumn("5M SYN cnt"),
        "5M API range": cc.TextColumn("5M API Range"),
        "5M SYN range": cc.TextColumn("5M SYN Range"),
        "5M Indicators": cc.TextColumn("5M Indicators"),
        "5M API Table": cc.CheckboxColumn("5M API Data Table"),
        "5M SYN Table": cc.CheckboxColumn("5M SYN Data Table"),
        "5M API+Ind": cc.CheckboxColumn("5M API + Indicators"),
        "5M SYN+Ind": cc.CheckboxColumn("5M SYN + Indicators"),
        "Chart": cc.CheckboxColumn("Open Chart"),
    }
    return cfg_map

def _disabled_noncheck_cols(df: pd.DataFrame):
    chk = {c for c in df.columns if "Table" in c or "+Ind" in c or c=="Chart"}
    return [c for c in df.columns if c not in chk]

def _render_section_table(title: str, symbols: list, filters: dict, default_interval="1D"):
    st.markdown(f"#### {title}")
    if not symbols:
        st.info("No contracts in this section.", icon="ℹ️")
        return
    max_rows = int(cfg.get("max_symbols_controls", 120))
    symbols = symbols[:max_rows]
    df = _build_editor_table(symbols, filters)
    if df.empty:
        st.info("No data available yet (DB will populate after first load / filters may exclude all).", icon="ℹ️")
        return
    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        column_config=_editor_column_config(),
        disabled=_disabled_noncheck_cols(df),
        height=min(520, 40 + 28*min(len(df), 12))
    )
    _render_actions_from_editor(edited, default_interval=default_interval)

# =============================================================================
# 21) UI tabs
# =============================================================================
with main_tabs[0]:
    st.markdown("### Markets")
    filters_mkt = _global_filters_ui("mkt")
    subtabs = st.tabs(["Outright", "Spread", "Fly"])
    with subtabs[0]:
        for code in products:
            with st.expander(f"{code} — Outrights", expanded=False):
                _render_section_table(f"{code} Outrights", contracts_by_product.get(code, {}).get('outr', []), filters_mkt)
    with subtabs[1]:
        for code in products:
            spr = contracts_by_product.get(code, {}).get('spr', {})
            if not spr: continue
            with st.expander(f"{code} — Spreads by Tenor", expanded=False):
                for t in sorted(spr.keys()):
                    syms = spr.get(t, [])
                    if not syms: continue
                    _render_section_table(f"{code} Spreads · {t}M", syms, filters_mkt)
    with subtabs[2]:
        for code in products:
            fly = contracts_by_product.get(code, {}).get('fly', {})
            if not fly: continue
            with st.expander(f"{code} — Flies by Tenor", expanded=False):
                for t in sorted(fly.keys()):
                    syms = fly.get(t, [])
                    if not syms: continue
                    _render_section_table(f"{code} Flies · {t}M", syms, filters_mkt)

with main_tabs[1]:
    st.markdown("### Intermarket")
    filters_im = _global_filters_ui("im")
    subtabs_im = st.tabs(["Outright", "Spread", "Fly"])
    with subtabs_im[0]:
        for (a,b), dd in inter_defs.items():
            key = f"{a}-{b}"
            with st.expander(f"{key} — Inter Outrights", expanded=False):
                _render_section_table(f"{key} Inter Outrights", dd.get('outr', []), filters_im)
    with subtabs_im[1]:
        for (a,b), dd in inter_defs.items():
            key = f"{a}-{b}"
            with st.expander(f"{key} — Inter Spreads by Tenor", expanded=False):
                for t in sorted(dd.get('spr', {}).keys()):
                    syms = dd['spr'].get(t, [])
                    if not syms: continue
                    _render_section_table(f"{key} Inter Spreads · {t}M", syms, filters_im)
    with subtabs_im[2]:
        for (a,b), dd in inter_defs.items():
            key = f"{a}-{b}"
            with st.expander(f"{key} — Inter Flies by Tenor", expanded=False):
                for t in sorted(dd.get('fly', {}).keys()):
                    syms = dd['fly'].get(t, [])
                    if not syms: continue
                    _render_section_table(f"{key} Inter Flies · {t}M", syms, filters_im)

# ---- Catalog tab (market-wise contract names) ----
@st.cache_data(ttl=300)
def db_fetch_catalog() -> pd.DataFrame:
    con = _duckdb_connect()
    if con is None: return pd.DataFrame()
    try:
        _duckdb_init(con)
        return con.execute("""
            SELECT market_or_pair, is_inter, symbol, strategy, tenor_months, contract_tag, curve_bucket,
                   left_leg, mid_leg, right_leg
            FROM contracts_catalog
            ORDER BY market_or_pair, strategy, tenor_months, symbol
        """).df()
    except Exception:
        return pd.DataFrame()
    finally:
        try: con.close()
        except Exception: pass

with main_tabs[2]:
    st.markdown("### Contracts Catalog (by Market / Pair)")
    dfcat = db_fetch_catalog()
    if dfcat.empty:
        st.info("Catalog will populate after initial load/persist cycle.", icon="ℹ️")
    else:
        q = st.text_input("Filter (symbol / market / tag / strategy)", "")
        if q.strip():
            qs = q.strip().lower()
            mask = (
                dfcat["symbol"].str.lower().str.contains(qs) |
                dfcat["market_or_pair"].str.lower().str.contains(qs) |
                dfcat["contract_tag"].fillna("").str.lower().str.contains(qs) |
                dfcat["strategy"].fillna("").str.lower().str.contains(qs)
            )
            dfv = dfcat[mask].copy()
        else:
            dfv = dfcat.copy()
        st.caption(f"Contracts: {len(dfv)}  •  Markets/Pairs: {dfv['market_or_pair'].nunique()}")
        st.dataframe(dfv, use_container_width=True, height=420)
        csv = dfv.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", data=csv, file_name="contracts_catalog.csv", mime="text/csv", use_container_width=True)

with main_tabs[3]:
    st.markdown("### Load Progress")
    if not st.session_state.progress['fetch']:
        st.caption("No in-flight fetches.")
    else:
        for code, per_itv in st.session_state.progress['fetch'].items():
            st.markdown(f"**{code}**")
            for itv, rec in per_itv.items():
                total = max(1, int(rec.get("total", 1)))
                done = int(rec.get("done", 0))
                st.write(f"{itv}: {done}/{total}")
                st.progress(min(1.0, done/total))
    st.markdown("---")
    st.caption(f"Last update: {st.session_state.last_update}  •  App v{APP_VERSION}")