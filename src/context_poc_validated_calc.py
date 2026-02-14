from initial_functions import fn_read_from_db, fn_write_to_db
import pandas as pd
from sqlalchemy import create_engine
import numpy as np


def fn_calculate_all_ranges(df, ticker, interval,CUTT_OFF=None):
    df = df.copy()
    df = df[df['TICKER']==ticker]
    df['DATETIME'] = pd.to_datetime(df['DATETIME'])

    if CUTT_OFF is not None:
        # 1. string → datetime (dayfirst=True)
        cut_dt = pd.to_datetime(CUTT_OFF, dayfirst=True)
    
        # 2. df['DATETIME'] timezone-aware ise CUTT_OFF'u aynı tz yap
        if df['DATETIME'].dt.tz is not None:
            cut_dt = cut_dt.tz_localize(df['DATETIME'].dt.tz)
    
        # 3. Filtrele
        df = df[df['DATETIME'] <= cut_dt]

    # En yeniden eskiye sırala
    df = df.sort_values('DATETIME', ascending=False).reset_index(drop=True)

    # Eğer filtre sonrası boşsa direkt boş dön
    if df.empty:
        return pd.DataFrame()

    end_datetime = df.iloc[0]['DATETIME']

    range_map = {
        "1day": pd.DateOffset(days=1),
        "1week": pd.DateOffset(weeks=1),
        "1months": pd.DateOffset(months=1),
        "3months": pd.DateOffset(months=3),
        "6months": pd.DateOffset(months=6),
        "1year": pd.DateOffset(years=1),
        "2year": pd.DateOffset(years=2)
    }

    results = []

    for range_name, offset in range_map.items():

        range_startdate = end_datetime - offset

        df_filter = df[
            (df['DATETIME'] >= range_startdate) &
            (df['DATETIME'] <= end_datetime)
        ].copy()

        if df_filter.empty:
            continue

        range_size = len(df_filter)

        highest_idx = df_filter['HIGH'].idxmax()
        highest_value = round(df_filter.loc[highest_idx, 'HIGH'], 2)
        highest_date = df_filter.loc[highest_idx, 'DATETIME']

        # Highest sonrası (artık CUTT_OFF kontrolü gerekmiyor)
        df_after = df_filter[df_filter['DATETIME'] >= highest_date]
        row_size_after_highest = len(df_after)

        end_datetime_filtered = df_filter['DATETIME'].max()

   

        results.append({
            "TICKER": ticker,
            "INTERVAL":interval,
            "RANGE_TYPE": range_name,
            "RANGE_STARTDATE": range_startdate,
            "RANGE_SIZE": range_size,
            "HIGHEST_VALUE": highest_value,
            "HIGHEST_DATE": highest_date,
            "ROW_SIZE_A_H": row_size_after_highest,
            "CUTT_OFF_DATE":CUTT_OFF,
            "END_DATETIME": end_datetime_filtered
        })

    df_res = pd.DataFrame(results)
    return df_res


#new best 

# ─────────────────────────────────────────────
# SENİN DOĞRULANMIŞ TV ALGORİTMAN (DEĞİŞMEDİ)
# ─────────────────────────────────────────────

def detect_tick_size(df):
    prices = pd.concat([df['OPEN'], df['HIGH'], df['LOW'], df['CLOSE']])
    diffs = np.diff(np.sort(prices.unique()))
    diffs = diffs[diffs > 0]
    return np.round(diffs.min(), 6)


def calculate_tv_frvp_v2(df, value_area_pct=68, row_size=1):

    df = df.copy()

    tick = detect_tick_size(df)
    price_step = tick * row_size

    price_min = np.floor(df['LOW'].min() / price_step) * price_step
    price_max = np.ceil(df['HIGH'].max() / price_step) * price_step

    price_levels = np.arange(price_min, price_max + price_step, price_step)
    hist = pd.Series(0.0, index=np.round(price_levels, 6))

    for _, row in df.iterrows():

        low = np.floor(row['LOW'] / price_step) * price_step
        high = np.ceil(row['HIGH'] / price_step) * price_step

        levels = np.arange(low, high + price_step, price_step)
        levels = np.round(levels, 6)

        if len(levels) == 0:
            continue

        volume = row['VOLUME']

        weights = np.linspace(1, 2, len(levels))
        weights = weights / weights.sum()

        hist.loc[levels] += volume * weights

    max_volume = hist.max()
    poc_candidates = hist[hist == max_volume].index.values
    poc = float(poc_candidates[len(poc_candidates) // 2])

    total_volume = hist.sum()
    target_volume = total_volume * (value_area_pct / 100)

    sorted_prices = hist.sort_index().index.values
    poc_pos = np.where(sorted_prices == poc)[0][0]

    included = {poc}
    cum_volume = hist.loc[poc]

    lower = poc_pos - 1
    upper = poc_pos + 1

    while cum_volume < target_volume:

        vol_down = hist.iloc[lower] if lower >= 0 else -1
        vol_up = hist.iloc[upper] if upper < len(hist) else -1

        if vol_up > vol_down:

            cum_volume += vol_up
            included.add(sorted_prices[upper])
            upper += 1

        elif vol_down > vol_up:

            cum_volume += vol_down
            included.add(sorted_prices[lower])
            lower -= 1

        else:

            if lower >= 0 and upper < len(hist):

                dist_down = abs(sorted_prices[lower] - poc)
                dist_up = abs(sorted_prices[upper] - poc)

                if dist_up <= dist_down:

                    cum_volume += vol_up
                    included.add(sorted_prices[upper])
                    upper += 1

                else:

                    cum_volume += vol_down
                    included.add(sorted_prices[lower])
                    lower -= 1

            elif upper < len(hist):

                cum_volume += vol_up
                included.add(sorted_prices[upper])
                upper += 1

            elif lower >= 0:

                cum_volume += vol_down
                included.add(sorted_prices[lower])
                lower -= 1

            else:
                break

    VAL = float(min(included))
    VAH = float(max(included))

    return {
        "POC": round(poc, 6),
        "VAL": round(VAL, 6),
        "VAH": round(VAH, 6)
    }


# ─────────────────────────────────────────────
# Main POC VAL VAH CALCILATION
# ─────────────────────────────────────────────

def fn_calculate_poc_frvp(
    ticker,
    df_ohlc,
    df_ranges,
    value_area_pct=68,
    row_size=1
):
    """
    df_ranges içindeki her satır için TradingView ile uyumlu
    POC VAL VAH hesaplar.

    df_ohlc columns:
        DATETIME, OPEN, HIGH, LOW, CLOSE, VOLUME, TICKER

    df_ranges columns:
        TICKER, HIGHEST_DATE, CUTT_OFF
    """

    df_ohlc = df_ohlc.copy()
    df_ranges = df_ranges.copy()

    # datetime formatı
    df_ohlc['DATETIME'] = pd.to_datetime(df_ohlc['DATETIME'])
    df_ranges['HIGHEST_DATE'] = pd.to_datetime(df_ranges['HIGHEST_DATE'])
    df_ranges['CUTT_OFF_DATE'] = pd.to_datetime(df_ranges['CUTT_OFF_DATE'], errors='coerce')

    # ticker filtre
    df_ohlc = df_ohlc[df_ohlc['TICKER'] == ticker].sort_values('DATETIME')

    # yeni kolonlar
    df_ranges['POC'] = np.nan
    df_ranges['VAL'] = np.nan
    df_ranges['VAH'] = np.nan

    # her range için hesapla
    for idx, row in df_ranges.iterrows():

        if row['TICKER'] != ticker:
            continue

        start_date = row['HIGHEST_DATE']

        # CUTT_OFF kontrolü
        if pd.isna(row['CUTT_OFF_DATE']):
            end_date = df_ohlc['DATETIME'].max()
        else:
            end_date = row['CUTT_OFF_DATE']

        # veri filtrele
        df_period = df_ohlc[
            (df_ohlc['DATETIME'] >= start_date) &
            (df_ohlc['DATETIME'] <= end_date)
        ]

        if df_period.empty:
            continue

        # TV hesaplama (AYNEN)
        result = calculate_tv_frvp_v2(
            df_period,
            value_area_pct=value_area_pct,
            row_size=row_size
        )

        df_ranges.at[idx, 'POC'] = result['POC']
        df_ranges.at[idx, 'VAL'] = result['VAL']
        df_ranges.at[idx, 'VAH'] = result['VAH']

    return df_ranges
