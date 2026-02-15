import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from kiteconnect import KiteConnect
from streamlit_autorefresh import st_autorefresh
import psycopg2
from psycopg2.extras import execute_batch
import time

# ==============================
# CONFIG (STREAMLIT SECRETS)
# ==============================
API_KEY = st.secrets["KITE_API_KEY"]
ACCESS_TOKEN = st.secrets["KITE_ACCESS_TOKEN"]
DATABASE_URL = st.secrets["DATABASE_URL"]

INDEX = "NIFTY"
STRIKE_RANGE = 800
MAX_CONTRACTS = 80

# ==============================
# DATABASE CONNECTION
# ==============================
try:
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    conn.autocommit = True
    cursor = conn.cursor()
except Exception as e:
    st.error("Database connection failed.")
    st.error(str(e))
    st.stop()

# ==============================
# SESSION STATE
# ==============================
if "volume_history" not in st.session_state:
    st.session_state.volume_history = {}

# ==============================
# KITE LOGIN
# ==============================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

st.set_page_config(layout="wide")
st.title("🚀 SMART OPTION CONTRACT SELECTOR – ELITE PRO")

st_autorefresh(interval=3000, key="refresh")

# ==============================
# GET NIFTY PRICE
# ==============================
def get_price():
    try:
        q = kite.ltp("NSE:NIFTY 50")
        return list(q.values())[0]["last_price"]
    except:
        return 0

# ==============================
# LOAD INSTRUMENTS
# ==============================
@st.cache_data(ttl=600)
def load_instruments():
    return pd.DataFrame(kite.instruments())

# ==============================
# VOLUME TRACKING
# ==============================
def update_volume_history(token, volume):
    now = time.time()
    if token not in st.session_state.volume_history:
        st.session_state.volume_history[token] = []
    st.session_state.volume_history[token].append((now, volume))
    st.session_state.volume_history[token] = [
        (t, v) for t, v in st.session_state.volume_history[token]
        if now - t <= 300
    ]

def calculate_spike(token):
    now = time.time()
    history = st.session_state.volume_history.get(token, [])

    def window(sec):
        relevant = [(t, v) for t, v in history if now - t <= sec]
        if len(relevant) >= 2:
            return relevant[-1][1] - relevant[0][1]
        return 0

    return {
        "vol_10s": window(10),
        "vol_30s": window(30),
        "vol_1m": window(60),
        "vol_3m": window(180),
        "vol_5m": window(300),
    }

# ==============================
# SAVE TO SUPABASE
# ==============================
def save_to_supabase(df):
    if df.empty:
        return

    query = """
    INSERT INTO nifty_option_snapshots (
        symbol, strike, type, ltp, volume, oi, oi_change, iv,
        vol_10s, vol_30s, vol_1m, vol_3m, vol_5m,
        volume_score, oi_score, oi_change_score, iv_score,
        vol_spike_score, score, confidence, volume_power
    ) VALUES (
        %(symbol)s, %(strike)s, %(type)s, %(ltp)s, %(volume)s,
        %(oi)s, %(oi_change)s, %(iv)s,
        %(vol_10s)s, %(vol_30s)s, %(vol_1m)s, %(vol_3m)s, %(vol_5m)s,
        %(volume_score)s, %(oi_score)s, %(oi_change_score)s,
        %(iv_score)s, %(vol_spike_score)s,
        %(score)s, %(confidence)s, %(volume_power)s
    )
    """

    data = df.to_dict("records")
    execute_batch(cursor, query, data)

# ==============================
# OPTION CHAIN ENGINE
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

    index_price = get_price()

    df = df[
        (df["strike"] > index_price - STRIKE_RANGE) &
        (df["strike"] < index_price + STRIKE_RANGE)
    ].head(MAX_CONTRACTS)

    tokens = df["instrument_token"].astype(str).tolist()

    try:
        quotes = kite.quote(tokens)
    except:
        return pd.DataFrame()

    rows = []

    for _, row in df.iterrows():
        token = str(row["instrument_token"])
        q = quotes.get(token, {})

        volume = q.get("volume", 0)

        update_volume_history(token, volume)
        spike = calculate_spike(token)

        rows.append({
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

    df_final = pd.DataFrame(rows)

    if df_final.empty:
        return df_final

    df_final["volume_score"] = df_final["volume"] / (df_final["volume"].max() or 1)
    df_final["oi_score"] = df_final["oi"] / (df_final["oi"].max() or 1)
    df_final["oi_change_score"] = df_final["oi_change"] / (df_final["oi_change"].max() or 1)
    df_final["iv_score"] = df_final["iv"] / (df_final["iv"].max() or 1)

    df_final["vol_spike_score"] = (
        df_final["vol_10s"] * 0.2 +
        df_final["vol_30s"] * 0.3 +
        df_final["vol_1m"] * 0.5
    )

    spike_norm = df_final["vol_spike_score"] / (df_final["vol_spike_score"].max() or 1)

    df_final["score"] = (
        df_final["volume_score"] * 0.2 +
        df_final["oi_score"] * 0.2 +
        df_final["oi_change_score"] * 0.2 +
        df_final["iv_score"] * 0.2 +
        spike_norm * 0.2
    )

    df_final["confidence"] = (df_final["score"] * 100).round(2)

    avg_spike = df_final["vol_spike_score"].mean()

    df_final["volume_power"] = np.where(
        (df_final["vol_10s"] > avg_spike * 1.5) |
        (df_final["vol_30s"] > avg_spike * 1.5) |
        (df_final["vol_1m"] > avg_spike * 1.5),
        "🚀 VOLUME BURST",
        ""
    )

    return df_final.sort_values("score", ascending=False)

# ==============================
# UI
# ==============================
price = get_price()
st.metric("NIFTY PRICE", price)

df = get_chain()

if df.empty:
    st.warning("No data available")
    st.stop()

# SAVE TO DATABASE
save_to_supabase(df)

st.dataframe(df.head(20), use_container_width=True)
