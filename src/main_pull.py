import pandas as pd
from context_pull import fn_pull_ticker_info, fn_read_from_db

# ----------------------------
# User parameters
# ----------------------------
INIT_PERIOD = "daily"  # "1min", "15min", "daily"
INIT_DAY = "2026-01-01"  # Start date for data fetch
# LST_TICKERS = ["AAPL", "MSFT", "GOOGL"]

df_ww = fn_read_from_db('worldwide_tickername_dataset')
lst_tickers = list(df_ww[df_ww['Country']=='USA']['Ticker'].unique())



for TICKER in lst_tickers:
    try:
        print(f'______{TICKER}')
        # MASTER TICKER PULL
        fn_pull_ticker_info(TICKER,INIT_DAY, INIT_PERIOD)
    except Exception as e:
        print(f'ERROR: {TICKER} | {e}')
        continue