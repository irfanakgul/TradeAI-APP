from tvDatafeed import TvDatafeed, Interval
def add_row_id(df,interval):

    # TIMESTAMP datetime değilse çevir
    ts = pd.to_datetime(df['DATETIME'])
    
    # time_id oluştur ve ROW_ID ata
    df['ROW_ID'] = (
        'ID_' +
        df['SYMBOL'].astype(str) +
        '_' +
        ts.dt.strftime('%Y%m%d_%H%M') +
        f'_{interval}')
    return df

df_out = tv.get_hist(
            symbol=symbol,
            exchange= "BIST",
            interval=Interval.in_1_minute,
            n_bars=200000
        )

df_out.columns = df_out.columns.str.upper()
    df_out['SYMBOL'] = symbol
    df_out['SOURCE'] = 'tvDatafeed'
    df_out['TIMESTAMP'] = df_out['DATETIME']
    df_out = add_row_id(df_out,'1min')
    df_out = df_out[['SYMBOL', 'TIMESTAMP','OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME','SOURCE','ROW_ID']]