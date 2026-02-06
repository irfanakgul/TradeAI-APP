from initial_functions import fn_distinct_write_to_db,fn_write_to_db, fn_get_latest_date_str
from context_fixed_range_calc import fn_frvp_calc
from context_ticker_pull import fn_pull_ticker_info

SOURCE_TABLE = 'tbl_ticker_actual_daily'
INTERVAL = 'daily'
PRICE_BIN = 1.0
VALUE_AREA_PERC = 0.70
LST_TICKERS = ['BIMAS.IS']
SAVED_TABLE = f'tbl_FRVP_{INTERVAL}'


for TICKER in LST_TICKERS:

    #identify last date
    str_pulled_last_date = fn_get_latest_date_str(SOURCE_TABLE,TICKER)
    
    #pull last days
    fn_pull_ticker_info(TICKER,str_pulled_last_date, INTERVAL)

    #calc FRVP
    df_res = fn_frvp_calc(SOURCE_TABLE = SOURCE_TABLE,
                    TICKER=TICKER,
                    INTERVAL = INTERVAL,
                    VALUE_AREA_PERC = VALUE_AREA_PERC,
                    PRICE_BIN = PRICE_BIN)

    #save to table
    fn_write_to_db(df_res, SAVED_TABLE, "replace")
    print(f'✅ {TICKER} FRVP ({INTERVAL}) has been calculated ***')

    print(f'✅✅✅ ALL DONE ✅✅✅')