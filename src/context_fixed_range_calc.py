from initial_functions import fn_read_from_db
import pandas as pd
import numpy as np
from collections import defaultdict
import json
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

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
        "1months": "30D",
        "3months": "90D",
        "6months": "180D",
        "1year": "365D",
        "2year": "730D"
    }

    VALID_INTERVALS = {
        "1day": {"1min", "15min"},
        "1week": {"1min", "15min"},
        "2week": {"15min"},
        "1months": {"15min", "daily"},
        "3months": {"daily"},
        "6months": {"daily"},
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
    df = df.sort_values("DATETIME").reset_index(drop=True)

    end_dt = df["DATETIME"].max()
    start_dt_range = end_dt - pd.Timedelta(RANGE_TO_PANDAS[range_])
    df_range = df[df["DATETIME"] >= start_dt_range]

    if df_range.empty:
        return None

    max_high = df_range["HIGH"].max()
    max_high_idx = df_range["HIGH"].idxmax()
    datetime_value = str(df_range.loc[max_high_idx, "DATETIME"])
    high_row_id = df_range.loc[max_high_idx, "ROW_ID"]

    high_value = max_high
    high_datetime = datetime_value
    df_calc = df[df["DATETIME"] >= high_datetime]

    if df_calc.empty:
        return None

    row_count_after_high = len(df_calc)

    # ---------------------------------
    # FRVP hesaplama
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
    # Weighted POC hesaplama (TradingView’e daha yakın)
    # ---------------------------------
    prices = np.array(sorted(frvp.keys()))
    volumes = np.array([frvp[p] for p in prices])
    poc = float(np.sum(prices * volumes) / np.sum(volumes))  # weighted POC

    # ---------------------------------
    # VAL / VAH hesaplama (POC etrafında cumulative volume)
    # ---------------------------------
    sorted_indices = np.argsort(prices)
    sorted_prices = prices[sorted_indices]
    sorted_volumes = volumes[sorted_indices]
    cum_vol = sorted_volumes.copy()
    total_vol = np.sum(sorted_volumes)

    # POC index
    poc_idx = np.searchsorted(sorted_prices, poc)

    lo = hi = poc_idx
    cum_vol_current = sorted_volumes[poc_idx]
    target_vol = total_vol * value_area_pct

    # TradingView mantığına benzer şekilde genişlet
    while cum_vol_current < target_vol:
        left_vol = sorted_volumes[lo - 1] if lo > 0 else 0
        right_vol = sorted_volumes[hi + 1] if hi < len(sorted_prices) - 1 else 0

        if right_vol >= left_vol and hi < len(sorted_prices) - 1:
            hi += 1
            cum_vol_current += right_vol
        elif lo > 0:
            lo -= 1
            cum_vol_current += left_vol
        else:
            break

    VAL = sorted_prices[lo]
    VAH = sorted_prices[hi]

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
        "VAL": VAL,
        "VAH": VAH,
        "PRICE_BIN": price_bin,
        "VAL_PERC": value_area_pct,
        "ROW_ID_FRVP": f'{high_row_id}_{range_}'
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
    "1months",
    "3months",
    "6months",
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
                  PRICE_BIN,
                  CUT_OFF):
    

    df = fn_read_from_db(f'{SOURCE_TABLE}')[[
        'TICKER','DATETIME','OPEN','HIGH','LOW','CLOSE',
        'VOLUME','INTERVAL','COUNTRY','ROW_ID']]

    df_ticker= df[df['TICKER'] == TICKER]

    # cut end data
    if CUT_OFF != None:
        
        df_ticker = df_ticker[
            pd.to_datetime(df_ticker['DATETIME'], errors='coerce')
            .dt.date
            <= pd.to_datetime(CUT_OFF).date()
            ]     
        print(f'⚠️ CUT_OFF is ACTIVE: {CUT_OFF}')

    df_res = compute_frvp_batch(
        df=df_ticker,
        interval=INTERVAL,
        price_bin=PRICE_BIN,
        value_area_pct=VALUE_AREA_PERC
    )
    runTime = f'{datetime.now().strftime("%d.%m.%Y")} - {datetime.now().strftime("%H:%M")}'
    df_res['RUNTIME'] = runTime
    return df_res