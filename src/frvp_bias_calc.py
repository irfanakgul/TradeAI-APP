import pandas as pd
import json
import numpy as np
from scipy.signal import find_peaks
from initial_functions import *

import pandas as pd
import json
import numpy as np
from scipy.signal import find_peaks

def add_frvp_trend_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    frvp_vwap = []
    frvp_skew = []
    frvp_peaks = []
    frvp_state = []
    trend_bias = []
    trend_score = []

    for _, row in df.iterrows():
        frvp = json.loads(row["FRVP_JSON"])

        prices = np.array([float(p) for p in frvp.keys()])
        volumes = np.array(list(frvp.values()))

        # --- FRVP VWAP ---
        vwap = np.sum(prices * volumes) / np.sum(volumes)
        frvp_vwap.append(vwap)

        # --- Skew (hacim üstte mi altta mı) ---
        above = volumes[prices > row["POC"]].sum()
        below = volumes[prices < row["POC"]].sum()

        if above + below > 0:
            skew = (above - below) / (above + below)
        else:
            skew = 0.0

        frvp_skew.append(skew)

        # --- Peak sayısı ---
        peaks, _ = find_peaks(volumes)
        frvp_peaks.append(len(peaks))

        # --- POC konumu ---
        va_range = row["VAH"] - row["VAL"]
        poc_pos = (row["POC"] - row["VAL"]) / va_range if va_range > 0 else 0.5

        # --- Market state + bias ---
        if poc_pos > 0.6 and skew > 0:
            state = "UPPER_ACCEPTANCE"
            bias = 1
        elif poc_pos < 0.4 and skew < 0:
            state = "LOWER_ACCEPTANCE"
            bias = -1
        else:
            state = "BALANCED"
            bias = 0

        frvp_state.append(state)
        trend_bias.append(bias)

        # --- Sürekli trend skoru ---
        score = 0.6 * skew + 0.4 * (poc_pos - 0.5)
        trend_score.append(score)

    # --- Yeni kolonları ekle ---
    df["FRVP_VWAP"] = frvp_vwap
    df["FRVP_SKEW"] = frvp_skew
    df["FRVP_PEAKS"] = frvp_peaks
    df["FRVP_STATE"] = frvp_state
    df["TREND_BIAS"] = trend_bias
    df["TREND_SCORE"] = trend_score

    return df

df = fn_read_from_db('tbl_FRVP_daily')

df_res = add_frvp_trend_columns(df)
fn_write_to_db(df_res,'BIAS','replace')