import pandas as pd
from initial_functions import fn_read_from_db, fn_write_to_db
from context_ticker_pull import fn_pull_ticker_info
from datetime import datetime

# ----------------------------
# User parameters
# ----------------------------
INIT_PERIOD = "daily"  # "1min", "15min", "daily"
INIT_DAY = "2024-01-01"  # Start date for data fetch
country = "Turkey"

df_ticker = fn_read_from_db('turkey_first500_company')
lst_tickers = list(df_ticker['TICKER'].unique())

for TICKER in lst_tickers:
    try:
        print(f'______{TICKER}_____')
        # MASTER TICKER PULL
        fn_pull_ticker_info(TICKER,INIT_DAY, INIT_PERIOD)

    except Exception as e:
        dic = {'TICKER':TICKER,
               'COUNTRY':country,
               'INTERVAL':INIT_PERIOD,
               'RUNTIME':datetime.now().strftime("%Y-%m-%d %H:%M")}
        df_err = pd.DataFrame([dic])
        fn_write_to_db(df=df_err, table_name=f'error_tickers', if_exists="append")
        print(f'ERROR: {TICKER} | {e}')
        continue

print('✅ ALL DONE')