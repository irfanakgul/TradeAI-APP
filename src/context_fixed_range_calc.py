from initial_functions import fn_read_from_db
import pandas as pd
import numpy as np
from collections import defaultdict
import json
import warnings
warnings.filterwarnings("ignore")


# -----------------------------------------
# Tek range için FRVP (HIGH bazlı)
# -----------------------------------------
def fn_compute_frvp_single_range(
    df: pd.DataFrame,
    interval: str,
    range_: str,
    price_bin: float,
    value_area_pct: float
):
    RANGE_TO_PANDAS = {
        "1day": "1D",
        "1week": "7D",
        "2week": "14D",
        "1mounth": "30D",
        "3mounth": "90D",
        "6mounth": "180D",
        "1year": "365D",
        "2year": "730D"
    }

    VALID_INTERVALS = {
        "1day": {"1min", "15min"},
        "1week": {"1min", "15min"},
        "2week": {"15min"},
        "1mounth": {"15min", "daily"},
        "3mounth": {"daily"},
        "6mounth": {"daily"},
        "1year": {"daily"},
        "2year": {"daily"},
    }

    if range_ not in RANGE_TO_PANDAS:
        warnings.warn(f"Gecersiz range: {range_}")
        return None

    if interval not in VALID_INTERVALS.get(range_, set()):
        warnings.warn(f"{range_} için {interval} uygun değil")
        return None

    df = df.copy()

    df["DATETIME"] = pd.to_datetime(df["DATETIME"])

    # 🔑 KRİTİK: kronolojik sırala
    df = df.sort_values("DATETIME").reset_index(drop=True)

    end_dt = df["DATETIME"].max()
    start_dt_range = end_dt - pd.Timedelta(RANGE_TO_PANDAS[range_])

    df_range = df[df["DATETIME"] >= start_dt_range]
    if df_range.empty:
        return None
    #
    max_high = df_range["HIGH"].max()
    max_high_idx = df_range["HIGH"].idxmax()
    datetime_value = str(df_range.loc[max_high_idx, "DATETIME"])
    high_row_id = df_range.loc[max_high_idx, "ROW_ID"]


    high_value = max_high
    high_datetime = datetime_value


    # 🔑 hesaplama HIGH sonrası
    df_calc = df[df["DATETIME"] >= high_datetime]

    if df_calc.empty:
        return None

    row_count_after_high = len(df_calc)

    # ---------------------------------
    # FRVP
    # ---------------------------------
    frvp = defaultdict(float)

    for _, row in df_calc.iterrows():
        low = row["LOW"]
        high = row["HIGH"]
        volume = row["VOLUME"]

        if volume <= 0 or pd.isna(volume):
            continue

        price_levels = np.arange(
            np.floor(low / price_bin) * price_bin,
            np.ceil(high / price_bin) * price_bin + price_bin,
            price_bin
        )

        if len(price_levels) == 0:
            continue

        vol_per_level = volume / len(price_levels)

        for p in price_levels:
            frvp[float(p)] += vol_per_level

    if not frvp:
        return None

    frvp = dict(frvp)

    # ---------------------------------
    # POC
    # ---------------------------------
    poc = max(frvp, key=frvp.get)

    # ---------------------------------
    # VALUE AREA
    # ---------------------------------
    total_vol = sum(frvp.values())
    target_vol = total_vol * value_area_pct

    prices = sorted(frvp.keys())
    poc_idx = prices.index(poc)

    cum_vol = frvp[poc]
    lo = hi = poc_idx

    while cum_vol < target_vol:
        left = frvp[prices[lo - 1]] if lo > 0 else 0
        right = frvp[prices[hi + 1]] if hi < len(prices) - 1 else 0

        if right >= left and hi < len(prices) - 1:
            hi += 1
            cum_vol += right
        elif lo > 0:
            lo -= 1
            cum_vol += left
        else:
            break

    return {
        "TICKER": df["TICKER"].iloc[0],
        "INTERVAL": interval,
        "RANGE": range_,
        "START_DATETIME": start_dt_range,
        "HIGH_DATETIME": high_datetime,
        "HIGH_VALUE": high_value,
        "HIGH_ROW_ID": high_row_id,
        "END_DATETIME": end_dt,
        "ROW_COUNT_AFTER_HIGH": row_count_after_high,
        "FRVP_JSON": json.dumps(frvp),
        "POC": poc,
        "VAL": prices[lo],
        "VAH": prices[hi],
        "PRICE_BIN": price_bin,
        "VAL_PERC": value_area_pct
    }


# -----------------------------------------
# BATCH RANGE
# -----------------------------------------
def compute_frvp_batch(
    df,
    interval,
    price_bin,
    value_area_pct
):

    rows = []

    ranges = [
    "1day",
    "1week",
    "1mounth",
    "3mounth",
    "6mounth",
    "1year",
    "2year"
]

    for r in ranges:
        res = fn_compute_frvp_single_range(
            df=df,
            interval=interval,
            range_=r,
            price_bin=price_bin,
            value_area_pct=value_area_pct
        )
        if res:
            rows.append(res)

    return pd.DataFrame(rows)

# -----------------------------------------
# ÇALIŞTIR
# -----------------------------------------

def fn_frvp_calc(SOURCE_TABLE,
                 TICKER,
                 INTERVAL,
                 VALUE_AREA_PERC,
                  PRICE_BIN):
    

    df = fn_read_from_db(f'{SOURCE_TABLE}')[[
        'TICKER','DATETIME','OPEN','HIGH','LOW','CLOSE',
        'VOLUME','INTERVAL','COUNTRY','ROW_ID']]

    df_ticker= df[df['TICKER'] == TICKER]

    df_res = compute_frvp_batch(
        df=df_ticker,
        interval=INTERVAL,
        price_bin=PRICE_BIN,
        value_area_pct=VALUE_AREA_PERC
    )
    return df_res
