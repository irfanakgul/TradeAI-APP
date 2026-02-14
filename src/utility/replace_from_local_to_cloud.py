# if the run doesn't work, then use this run code in terminal: 
# python /Users/irfanakgul/Desktop/FPM_AI_WEB_APP/model_exe/utilty/replace_tables_from_local_to_cloud.py

sqlite_path = "/Users/irfanakgul/Desktop/TradeAI-APP/src/trade_ai.db"

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import sqlite3,re


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
    print(conn.execute(text("SELECT current_user, current_database(), version()")).fetchone())


def fn_read_data_db(tableName, conn):
    # basit güvenlik: sadece harf/rakam/_ izin ver
    if not re.fullmatch(r"[A-Za-z0-9_]+", tableName):
        raise ValueError("Invalid table name")

    query = f'SELECT * FROM {tableName}'
    return pd.read_sql(query, conn)


conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()


manual_table_list = []

if len(manual_table_list)>0:
    print(f"Size: {len(manual_table_list)}")
    tables = manual_table_list


print(len(tables))
for table in tables:
    
    if len(manual_table_list)>0:
        str_table = table
    else:
        str_table = table[0]
        
    df = fn_read_data_db(str_table,conn)
    print(f"{str_table} | {len(df)}")
    df.to_sql(f'{str_table}', engine, if_exists="replace", index=False)

print('**DONE**')