import streamlit as st
import pandas as pd
import numpy as np
from kiteconnect import KiteConnect
from streamlit_autorefresh import st_autorefresh
from supabase import create_client
import time

# ==============================
# LOAD SECRETS
# ==============================
KITE_API_KEY = st.secrets["KITE_API_KEY"]
KITE_ACCESS_TOKEN = st.secrets["KITE_ACCESS_TOKEN"]

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_ANON_KEY = st.secrets["SUPABASE_ANON_KEY"]

INDEX = "NIFTY"
STRIKE_RANGE = 800
MAX_CONTRACTS = 60  # Keep lower for performance

# ==============================
# INIT CONNECTIONS
# ==============================
kite = KiteConnect(api_key=KITE_API_KEY)
kite.set_access_token(KITE_ACCESS_TOKEN)

supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# ==============================
# SESSION STATE
# ==============================
if "volume_history" not in st.session_state:
    st.session_state.volume_history = {}

if "last_push_time" not in st.session_state:
    st.session_state.last_push_time = 0

# ==============================
# PAGE CONFIG
# ==============================
st.set_page_config(layout="wide")
st.title("🚀 SMART OPTION CONTRACT SELECTOR – ELITE PRO")

# Refresh every 5 seconds
st_autorefresh(interval=5000, key="refresh")

# ==============================
# GET INDEX PRICE
# ==============================
def get_price():
    try:
        q = kite.ltp("NSE:NIFTY 50")
        return list(q.values())[0]["last_price"]
    except:
        st.error("Kite session expired. Generate new access token.")
        st.stop()

# ==============================
# LOAD INSTRUMENTS
# ==============================
@st.cache_data(ttl=600)
def load_instruments():
    return pd.DataFrame(kite.instruments())

# ==============================
# VOLUME ENGINE
# ==============================
def update_volume(token, volume):
    now = time.time()

    if token not in st.session_state.volume_history:
        st.session_state.volume_history[token] = []

    st.session_state.volume_history[token].append((now, volume))

    # Keep only last 5 minutes
    st.session_state.volume_history[token] = [
        (t, v)
        for t, v in st.session_state.volume_history[token]
        if now - t <= 300
    ]

def get_spike(token):
    now = time.time()
    history = st.session_state.volume_history.get(token, [])

    def calc(sec):
        data = [(t, v) for t, v in history if now - t <= sec]
        if len(data) >= 2:
            return data[-1][1] - data[0][1]
        return 0

    return {
        "vol_10s": calc(10),
        "vol_30s": calc(30),
        "vol_1m": calc(60),
        "vol_3m": calc(180),
        "vol_5m": calc(300),
    }

# ==============================
# BUILD OPTION CHAIN
# ==============================
def get_chain():
    instruments = load_instruments()

    df = instruments[
        (instruments["name"] == INDEX) &
        (instruments["segment"].str.contains("OPT"))
    ]

    if df.empty:
        return pd.DataFrame()

    expiry = sorted(df["expiry"].unique())[0]
    df = df[df["expiry"] == expiry]

    price = get_price()

    df = df[
        (df["strike"] > price - STRIKE_RANGE) &
        (df["strike"] < price + STRIKE_RANGE)
    ].head(MAX_CONTRACTS)

    tokens = df["instrument_token"].astype(str).tolist()

    try:
        quotes = kite.quote(tokens)
    except:
        return pd.DataFrame()

    data = []

    for _, row in df.iterrows():
        token = str(row["instrument_token"])
        q = quotes.get(token, {})

        volume = q.get("volume", 0)
        update_volume(token, volume)
        spike = get_spike(token)

        data.append({
            "symbol": row["tradingsymbol"],
            "strike": row["strike"],
            "type": row["instrument_type"],
            "ltp": q.get("last_price", 0),
            "volume": volume,
            "oi": q.get("oi", 0),
            "oi_change": q.get("oi_day_high", 0) - q.get("oi_day_low", 0),
            "iv": q.get("implied_volatility", 0),
            **spike
        })

    df_final = pd.DataFrame(data)

    if df_final.empty:
        return df_final

    # Normalize scores safely
    def norm(col):
        max_val = df_final[col].max()
        return df_final[col] / max_val if max_val != 0 else 0

    df_final["volume_score"] = norm("volume")
    df_final["oi_score"] = norm("oi")
    df_final["oi_change_score"] = norm("oi_change")
    df_final["iv_score"] = norm("iv")

    df_final["vol_spike_score"] = (
        df_final["vol_10s"] * 0.2 +
        df_final["vol_30s"] * 0.3 +
        df_final["vol_1m"] * 0.5
    )

    spike_norm = norm("vol_spike_score")

    df_final["score"] = (
        df_final["volume_score"] * 0.2 +
        df_final["oi_score"] * 0.2 +
        df_final["oi_change_score"] * 0.2 +
        df_final["iv_score"] * 0.2 +
        spike_norm * 0.2
    )

    df_final["confidence"] = (df_final["score"] * 100).round(2)

    return df_final.sort_values("score", ascending=False)

# ==============================
# PUSH TO SUPABASE (1 MINUTE)
# ==============================
def push_to_supabase(df):
    try:
        records = df.to_dict(orient="records")
        supabase.table("nifty_option_snapshots").insert(records).execute()
    except Exception as e:
        st.error(f"Supabase Insert Error: {e}")

# ==============================
# MAIN
# ==============================
price = get_price()
st.metric("NIFTY PRICE", price)

df = get_chain()

if df.empty:
    st.warning("No data available")
    st.stop()

# Push once per 60 seconds (safe control)
current_time = time.time()
if current_time - st.session_state.last_push_time > 60:
    columns_required = [
        "symbol", "strike", "type", "ltp", "volume", "oi",
        "oi_change", "iv",
        "vol_10s", "vol_30s", "vol_1m", "vol_3m", "vol_5m",
        "volume_score", "oi_score", "oi_change_score",
        "iv_score", "vol_spike_score", "score",
        "confidence"
    ]
    push_to_supabase(df[columns_required])
    st.session_state.last_push_time = current_time

st.subheader("🔥 BEST CONTRACT")
st.dataframe(df.head(1), use_container_width=True)

st.subheader("📈 Top 20 Contracts")
st.dataframe(df.head(20), use_container_width=True)
