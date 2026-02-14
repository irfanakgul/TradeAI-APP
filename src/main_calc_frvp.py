from initial_functions import fn_distinct_write_to_db,fn_write_to_db, fn_get_latest_date_str,fn_read_from_db
# from context_fixed_range_calc import fn_frvp_calc
from context_ticker_pull import fn_pull_ticker_info
# from context_poc_validated_calc import fn_calculate_poc_frvp,fn_calculate_all_ranges
from context_poc_validated_calc import *



SOURCE_TABLE = 'tbl_ticker_actual_1m'
INTERVAL = '1min'
PRICE_BIN = 1.0
VALUE_AREA_PERC = 68    # 68,  70
LST_TICKERS = ['ULKER.IS','DOAS.IS','CIMSA.IS','ALKA.IS'] # 'ASUZU.IS',
SAVED_TABLE = f'tbl_FRVP_{INTERVAL}'
SAVING_TYPE = 'append'   #append or replace
CUT_OFF = None


#read source data
df_raw = fn_read_from_db(SOURCE_TABLE)


for TICKER in LST_TICKERS:
    print(f'------| {TICKER} |-------')

    #identify last date
    str_pulled_last_date = fn_get_latest_date_str(SOURCE_TABLE,TICKER)
    
    #pull last days
    fn_pull_ticker_info(TICKER,str_pulled_last_date, INTERVAL)

    df_res = fn_calculate_all_ranges(df_raw, TICKER,'1min', CUTT_OFF=CUT_OFF)
    
    df_out = fn_calculate_poc_frvp(
        ticker=TICKER,
        df_ohlc=df_raw,        # 1 dakikalık veri
        df_ranges=df_res,   # HIGHEST_DATE / CUTT_OFF içeren df
        value_area_pct=VALUE_AREA_PERC,
        row_size=1)

    #save to table
    # fn_distinct_write_to_db(df=df_res, table_name=SAVED_TABLE,dist_col_name="ROW_ID_FRVP",if_exists= 'append')
    fn_write_to_db(df=df_out, table_name=SAVED_TABLE,if_exists= SAVING_TYPE)

    print(f'✅ {TICKER} FRVP ({INTERVAL}) has been calculated ***')
print(f'✅✅✅ ALL DONE ✅✅✅')