import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# ----------------------------
# User parameters
# ----------------------------
INIT_PERIOD = "daily"  # "1min", "15min", "daily"
INIT_DAY = "2026-01-01"  # Start date for data fetch
TICKERS = ["AAPL", "MSFT", "GOOGL"]

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
    StructField("DATE_TIME", StringType()),  # For intraday, store as string with timestamp
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
    StructField("USERNAME", StringType()), #pulling username



])

# ----------------------------
# Create empty DataFrame
# ----------------------------
final_df = spark.createDataFrame([], schema)

# ----------------------------
# Loop through tickers
# ----------------------------
for t in TICKERS:
    ticker = yf.Ticker(t)

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
    if interval == "1d":
        hist = ticker.history(start=INIT_DAY, interval=interval)

    else:
        hist = ticker.history(period=period, interval=interval)


    if hist.empty:
        continue

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

    if interval == "1d":
        date_ = hist.Date
    else:
        date_ = hist.Datetime


    pdf = pd.DataFrame({
        "TICKER": t,
        "DATE_TIME": date_,    #hist.Datetime,  # string for intraday timestamps
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
pandas_df = final_df.toPandas()
pandas_df.to_excel('res.xlsx', index=False)