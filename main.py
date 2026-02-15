import os
import time
import pytz
import pandas as pd
from datetime import datetime, time as dtime
from supabase import create_client, Client

# =====================================
# SUPABASE CONFIG (SET IN RENDER ENV)
# =====================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # Use SERVICE ROLE KEY

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# =====================================
# TIMEZONE
# =====================================
IST = pytz.timezone("Asia/Kolkata")

# =====================================
# MARKET HOURS CHECK
# =====================================
def is_market_open():
    now = datetime.now(IST)

    # Monday=0, Sunday=6
    if now.weekday() > 4:
        return False

    market_start = dtime(9, 0)
    market_end = dtime(15, 30)

    return market_start <= now.time() <= market_end


# =====================================
# SUPABASE INSERT FUNCTION
# =====================================
def insert_option_data(df: pd.DataFrame):

    if df.empty:
        return

    if not is_market_open():
        print("Market closed. Skipping insert.")
        return

    df = df.copy()
    df["timestamp"] = datetime.now(IST)

    records = df.to_dict(orient="records")

    try:
        response = supabase.table("option_ticks").insert(records).execute()
        print(f"Inserted {len(records)} rows at {datetime.now(IST)}")
    except Exception as e:
        print("Insert failed:", e)


# =====================================
# MOCK FUNCTION (REPLACE WITH YOUR ENGINE)
# =====================================
def generate_dataframe():
    """
    Replace this with your actual option chain logic.
    This is just example format.
    """

    data = [
        {
            "symbol": "NIFTY2621724850CE",
            "strike": 24850.0,
            "type": "CE",
            "ltp": 700.0,
            "volume": 3380,
            "oi": 10595,
            "oi_change": 1430,
            "iv": 0,
            "vol_10s": 0,
            "vol_30s": 0,
            "vol_1m": 0,
            "vol_3m": 0,
            "vol_5m": 0,
            "volume_score": 0.000007,
            "oi_score": 0.00053,
            "oi_change_score": 0.000064,
            "iv_score": 0,
            "vol_spike_score": 0,
            "score": 0.00012,
            "confidence": 0.01,
            "volume_power": ""
        }
    ]

    return pd.DataFrame(data)


# =====================================
# MAIN LOOP (RENDER WORKER)
# =====================================
def run_worker():
    print("Render Worker Started...")

    while True:
        try:
            df = generate_dataframe()  # Replace with your real engine
            insert_option_data(df)
        except Exception as e:
            print("Loop error:", e)

        time.sleep(3)  # 3 second interval


# =====================================
# START
# =====================================
if __name__ == "__main__":
    run_worker()
