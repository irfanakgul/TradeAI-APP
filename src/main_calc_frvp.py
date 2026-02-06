from initial_functions import fn_distinct_write_to_db,fn_write_to_db
from context_fixed_range_calc import fn_frvp_calc

SOURCE_TABLE = 'tbl_ticker_actual_daily'
INTERVAL = 'daily'
PRICE_BIN = 1.0
VALUE_AREA_PERC = 0.70
TICKER = 'BIMAS.IS'
SAVED_TABLE = f'tbl_FRVP_{INTERVAL}'

df_res = fn_frvp_calc(SOURCE_TABLE = SOURCE_TABLE,
                 TICKER=TICKER,
                 INTERVAL = INTERVAL,
                 VALUE_AREA_PERC = VALUE_AREA_PERC,
                  PRICE_BIN = PRICE_BIN)

fn_write_to_db(df_res, SAVED_TABLE, "replace")
print(f'✅ *** {TICKER} FRVP ({INTERVAL}) has been calculated ***')