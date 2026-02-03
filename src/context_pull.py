import os
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sqlalchemy import create_engine

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# #db connection
engine = create_engine('sqlite:///trade_ai.db')

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


#save to db
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


#add unique row id
def add_row_id_spark(df):
    """
    Adds ROW_ID column to Spark DataFrame.
    Uses original DATETIME string to avoid timezone shifts.
    """

    # Build time_param directly from string
    df = df.withColumn(
        "TIME_PARAM",
        F.concat(
            F.substring("DATETIME", 1, 4),   # YYYY
            F.substring("DATETIME", 6, 2),   # MM
            F.substring("DATETIME", 9, 2),   # DD
            F.lit("_"),
            F.substring("DATETIME", 12, 2),  # HH
            F.substring("DATETIME", 15, 2),  # MM
        )
    )

    # Create ROW_ID
    df = df.withColumn(
        "ROW_ID",
        F.concat(
            F.lit("ID_"),
            F.col("TICKER"),
            F.lit("_"),
            F.col("TIME_PARAM"),
            F.lit("_"),
            F.col("INTERVAL")
        )
    )

    df = df.drop("TIME_PARAM")

    return df

# MASTER FUNC
def fn_pull_ticker_info(TICKER,INIT_DAY, INIT_PERIOD):
    
    # ----------------------------
    # Initialize Spark session
    # ----------------------------
    spark = SparkSession.builder \
        .appName("YahooFinanceFullData") \
        .getOrCreate()
    
    # ----------------------------
    # Define Spark schema (uppercase + _)
    # ----------------------------
    schema = StructType([
        StructField("TICKER", StringType()),
        StructField("DATETIME", StringType()),  # For intraday, store as string with timestamp
        StructField("OPEN", DoubleType()),
        StructField("HIGH", DoubleType()),
        StructField("LOW", DoubleType()),
        StructField("CLOSE", DoubleType()),
        StructField("ADJ_CLOSE", DoubleType()),
        StructField("VOLUME", LongType()),
        StructField("DIVIDENDS", DoubleType()),
        StructField("STOCK_SPLITS", DoubleType()),
        StructField("SHORT_NAME", StringType()),
        StructField("SECTOR", StringType()),
        StructField("INDUSTRY", StringType()),
        StructField("COUNTRY", StringType()),
        StructField("CURRENCY", StringType()),
        StructField("EXCHANGE", StringType()),
        StructField("MARKET_CAP", LongType()),
        StructField("BETA", DoubleType()),
        StructField("SHARES_OUTSTANDING", LongType()),
        StructField("TRAILING_PE", DoubleType()),
        StructField("FORWARD_PE", DoubleType()),
        StructField("PRICE_TO_BOOK", DoubleType()),
        StructField("ENTERPRISE_VALUE", LongType()),
        StructField("PROFIT_MARGINS", DoubleType()),
        StructField("ROE", DoubleType()),
        StructField("DEBT_TO_EQUITY", DoubleType()),
        StructField("FREE_CASHFLOW", LongType()),
        StructField("OPERATING_CASHFLOW", LongType()),
        StructField("INTERVAL", StringType()),  # New column for init_period

        StructField("RUNTIME", StringType()),
        StructField("USERNAME", StringType()) #pulling username

    ])

    # ----------------------------
    # Create empty DataFrame
    # ----------------------------
    final_df = spark.createDataFrame([], schema)

    # FOR CHECKING OF EXISTING ROW_ID
    lst_exist_row_ids = list(fn_read_from_db(f'raw_ticker_{INIT_PERIOD}')['ROW_ID'])
    lst_exist_row_ids = [x.strip() for x in lst_exist_row_ids]

    # ----------------------------
    # Loop through tickers
    # ----------------------------
    # for t in LST_TICKERS:
    ticker = yf.Ticker(TICKER)

    # ----------------------------
    # Determine interval & start date
    # ----------------------------
    if INIT_PERIOD.lower() == "1min":
        interval = "1m"
        period = "7d"  # max for 1min interval
    elif INIT_PERIOD.lower() == "15min":
        interval = "15m"
        period = "60d"  # max for 15min interval
    elif INIT_PERIOD.lower() == "daily":
        interval = "1d"
        period = None  # we will use start date instead
    else:
        interval = "1d"
        period = None

    # ----------------------------
    # Fetch historical data
    # ----------------------------
    if INIT_PERIOD == "daily":
        hist = ticker.history(start=INIT_DAY, interval=interval)
        adj_date = 'Date'
        interval = INIT_PERIOD

    else: # 1min and 15min
        hist = ticker.history(period=period, interval=interval)
        adj_date = 'Datetime'

    hist.reset_index(inplace=True)

    # ----------------------------
    # Get company info
    # ----------------------------
    info = ticker.info

    # ----------------------------
    # Runtime timestamp
    # ----------------------------
    runtime = datetime.now().strftime("%m-%d-%Y | %H:%M")

    # ----------------------------
    # Map data to Pandas DataFrame, fill missing with np.nan
    # ----------------------------

    pdf = pd.DataFrame({
        "TICKER": TICKER,
        "DATETIME": pd.to_datetime(hist[adj_date]),    #hist.Datetime,  # string for intraday timestamps
        "OPEN": hist.get("Open", np.nan),
        "HIGH": hist.get("High", np.nan),
        "LOW": hist.get("Low", np.nan),
        "CLOSE": hist.get("Close", np.nan),
        "ADJ_CLOSE": hist.get("Adj Close", np.nan),
        "VOLUME": hist.get("Volume", np.nan),
        "DIVIDENDS": hist.get("Dividends", np.nan),
        "STOCK_SPLITS": hist.get("Stock Splits", np.nan),
        "SHORT_NAME": info.get("shortName", np.nan),
        "SECTOR": info.get("sector", np.nan),
        "INDUSTRY": info.get("industry", np.nan),
        "COUNTRY": info.get("country", np.nan),
        "CURRENCY": info.get("currency", np.nan),
        "EXCHANGE": info.get("exchange", np.nan),
        "MARKET_CAP": info.get("marketCap", np.nan),
        "BETA": info.get("beta", np.nan),
        "SHARES_OUTSTANDING": info.get("sharesOutstanding", np.nan),
        "TRAILING_PE": info.get("trailingPE", np.nan),
        "FORWARD_PE": info.get("forwardPE", np.nan),
        "PRICE_TO_BOOK": info.get("priceToBook", np.nan),
        "ENTERPRISE_VALUE": info.get("enterpriseValue", np.nan),
        "PROFIT_MARGINS": info.get("profitMargins", np.nan),
        "ROE": info.get("returnOnEquity", np.nan),
        "DEBT_TO_EQUITY": info.get("debtToEquity", np.nan),
        "FREE_CASHFLOW": info.get("freeCashflow", np.nan),
        "OPERATING_CASHFLOW": info.get("operatingCashflow", np.nan),
        "INTERVAL": INIT_PERIOD,
        "RUNTIME": runtime,
        "USERNAME": 'irfanA'
    })

    # ----------------------------
    # Convert Pandas -> Spark DF and append
    # ----------------------------
    spark_df = spark.createDataFrame(pdf, schema=schema)
    final_df = final_df.unionByName(spark_df)

    # ----------------------------
    # Show schema and preview
    # ----------------------------
    final_df.printSchema()

    #add row id
    final_df = add_row_id_spark(final_df)


    pds = final_df.toPandas()

    pds['ROW_ID'] = pds['ROW_ID'].str.strip()
    pds_dist = pds[~pds['ROW_ID'].isin(lst_exist_row_ids)]

    print(f'** New record count: {len(pds_dist)}')

    fn_write_to_db(df=pds_dist, table_name=f'raw_ticker_{interval}', if_exists="append")

# MASTER TICKER PULL
# fn_pull_ticker_info(LST_TICKERS,INIT_DAY, INIT_PERIOD)
