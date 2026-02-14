# #db connection
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
import re

engine_local = create_engine('sqlite:///trade_ai.db')

# # connection into cloud db (central)
def cloud_connection():
    url = URL.create(
    "postgresql+psycopg2",
    username="irfan_admin",
    password="TradeAPP_IA_2026@",
    host="95.216.148.216",
    port=5432,
    database="trade_app",
)

    engine = create_engine(url, pool_pre_ping=True)

    with engine.connect() as conn:
        print('========|',conn.execute(text("SELECT current_user, current_database()")).fetchone(),'|========')

    print('*** ✅ SUCCESSFUL CLOUD CONNECTION ⛓️ ***')
    
    return engine

engine = cloud_connection()

#read from db
def fn_read_from_db(table_name, columns=None, where=None):
    # SELECT cümlesi oluştur
    cols = ", ".join(columns) if columns else "*"
    sql = f"SELECT {cols} FROM {table_name}"
    
    if where:
        sql += f" WHERE {where}"
    
    # pandas ile SQL sorgusu çalıştır
    df = pd.read_sql(sql, con=engine_local)
    return df


#save all rows to db
def fn_write_to_db(df, table_name, if_exists="replace"):
    df.to_sql(
        name=table_name,
        con=engine_local,
        if_exists=if_exists,
        index=False,
        # method="multi",  # performans için
        chunksize=500
    )
    print(f'*** SAVED TO DB: {table_name} | {if_exists}')


# distinct rows saving into db
def fn_distinct_write_to_db(df, table_name,dist_col_name, if_exists):
    lst_exist_rows = list(fn_read_from_db(table_name=table_name)[dist_col_name])

    if dist_col_name not in lst_exist_rows:
        df_dist = df[~df[dist_col_name].isin(lst_exist_rows)]
        fn_write_to_db(df_dist,table_name,if_exists)
    else:
        print(f'!{dist_col_name} exist!')

# take latest date for update starting
def fn_get_latest_date_str(table_name, TICKER):
    df = fn_read_from_db(table_name)[['DATETIME','TICKER']]
    df = df[df['TICKER']==TICKER]
    dt = pd.to_datetime(df["DATETIME"])
    latest_date = dt.max()
    # YYYY-MM-DD formatında string döndür
    str_last_date = latest_date.strftime("%Y-%m-%d")
    print(f'>>> {TICKER} last date: {str_last_date}')
    return str_last_date


def fn_max_date_calc(tablename,colname):
    str_max_date = pd.read_sql(f"SELECT MAX({colname}) FROM {tablename}", engine_local)
    return str_max_date