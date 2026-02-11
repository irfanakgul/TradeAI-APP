# #db connection
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
import re

engine_local = create_engine('sqlite:///trade_ai.db')

# connection into cloud db (central)
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



def read_sql_case_safe(engine, query: str):
    """
    Çok temel SELECT sorgularında tablo/kolonları DB şemasından okuyup doğru şekilde quote eder.
    Şu formu hedefler:
      SELECT col1, col2 FROM table ...
      SELECT * FROM table ...
    Daha karmaşık JOIN/alias sorgularda sınırlı kalabilir.
    """
    q = query.strip().rstrip(";")

    m = re.match(r'(?is)^\s*select\s+(?P<select>.+?)\s+from\s+(?P<table>[A-Za-z0-9_]+)\s*(?P<rest>.*)$', q)
    if not m:
        # tanıyamazsa aynen çalıştır
        return pd.read_sql_query(query, engine)

    select_part = m.group("select").strip()
    table = m.group("table").strip()
    rest = m.group("rest") or ""

    # tablonun gerçek kolon adlarını çek
    cols = pd.read_sql_query(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
        ORDER BY ordinal_position
    """), engine, params={"t": table})["column_name"].tolist()

    # kolonu case-insensitive eşle
    col_map = {c.lower(): c for c in cols}

    if select_part == "*":
        fixed_select = "*"
    else:
        raw_cols = [c.strip() for c in select_part.split(",")]
        fixed_cols = []
        for rc in raw_cols:
            # fonksiyon/alias gibi şeyler varsa dokunmayalım
            if re.search(r'\(|\)|\s+as\s+|\s', rc, flags=re.I):
                fixed_cols.append(rc)
                continue

            real = col_map.get(rc.lower())
            if real:
                fixed_cols.append(f'"{real}"')
            else:
                fixed_cols.append(rc)
        fixed_select = ", ".join(fixed_cols)

    fixed_query = f'SELECT {fixed_select} FROM "{table}" {rest}'.strip()
    return pd.read_sql_query(fixed_query, engine)


#read data    
def fn_read_data_cloud(tableName):
    # basit güvenlik: sadece harf/rakam/_ izin ver
    if not re.fullmatch(r"[A-Za-z0-9_]+", tableName):
        raise ValueError("Invalid table name")

    query = f'SELECT * FROM "{tableName}"'
    
    return read_sql_case_safe(engine, query)



#read from db
def fn_read_from_db(table_name, columns=None, where=None):

    # SELECT cümlesi oluştur
    cols = ", ".join(columns) if columns else "*"
    sql = f"SELECT {cols} FROM {table_name}"
    
    if where:
        sql += f" WHERE {where}"
    
    # pandas ile SQL sorgusu çalıştır
    df = pd.read_sql(sql, con=engine)
    return df


#save all rows to db
def fn_write_to_db(df, table_name, if_exists="replace"):
    df.to_sql(
        name=table_name,
        con=engine,
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
    str_max_date = pd.read_sql(f"SELECT MAX({colname}) FROM {tablename}", engine)
    return str_max_date



