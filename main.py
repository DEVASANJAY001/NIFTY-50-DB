import pandas as pd
import numpy as np
import time
from kiteconnect import KiteConnect

# ==============================
# CONFIG
# ==============================
API_KEY = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

INDEX = "NIFTY"
STRIKE_RANGE = 800
MAX_CONTRACTS = 80

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ==============================
# VOLUME MEMORY (In-Memory)
# ==============================
volume_history = {}

# ==============================
# PRICE FETCH
# ==============================
def get_price():
    try:
        symbol = "NSE:NIFTY 50"
        q = kite.ltp(symbol)
        return list(q.values())[0]["last_price"]
    except:
        return 0

# ==============================
# LOAD INSTRUMENTS
# ==============================
def load_instruments():
    return pd.DataFrame(kite.instruments())

# ==============================
# VOLUME SPIKE ENGINE
# ==============================
def update_volume_history(token, current_volume):
    now = time.time()

    if token not in volume_history:
        volume_history[token] = []

    volume_history[token].append((now, current_volume))

    # Keep last 5 minutes only
    volume_history[token] = [
        (t, v) for t, v in volume_history[token]
        if now - t <= 300
    ]


def calculate_volume_spike(token):
    now = time.time()
    history = volume_history.get(token, [])

    def volume_window(seconds):
        relevant = [(t, v) for t, v in history if now - t <= seconds]
        if len(relevant) >= 2:
            return relevant[-1][1] - relevant[0][1]
        return 0

    return {
        "vol_10s": volume_window(10),
        "vol_30s": volume_window(30),
        "vol_1m": volume_window(60),
        "vol_3m": volume_window(180),
        "vol_5m": volume_window(300),
    }

# ==============================
# MAIN DATA MODEL
# ==============================
def generate_option_dataframe():

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

    data = []

    for _, row in df.iterrows():
        token = str(row["instrument_token"])
        q = quotes.get(token, {})

        volume = q.get("volume", 0)

        update_volume_history(token, volume)
        spike = calculate_volume_spike(token)

        data.append({
            "symbol": row["tradingsymbol"],
            "strike": row["strike"],
            "type": row["instrument_type"],
            "ltp": q.get("last_price", 0),
            "volume": volume,
            "oi": q.get("oi", 0),
            "oi_change": q.get("oi_day_high", 0) - q.get("oi_day_low", 0),
            "iv": q.get("implied_volatility", 0),

            "vol_10s": spike["vol_10s"],
            "vol_30s": spike["vol_30s"],
            "vol_1m": spike["vol_1m"],
            "vol_3m": spike["vol_3m"],
            "vol_5m": spike["vol_5m"],
        })

    df_final = pd.DataFrame(data)

    if df_final.empty:
        return df_final

    # ==============================
    # SCORING MODEL
    # ==============================
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
