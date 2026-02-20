import requests
import pandas as pd
import time
import sqlite3
from datetime import datetime
from initial_functions import fn_distinct_write_to_db,engine_local

API_KEY = 'Ey_rYa3wWU4iUjFxSjS4o5hn9p9tXLUa'
START_DATE = '2026-01-01'

# --- AYARLAR ---
SYMBOLS = [
    "MMM","AOS","ABT","ABBV","ACN","ATVI","AYI","ADBE","AAP","AMD",
    "AES","AFL","A","APD","AKAM","ALK","ALB","ARE","ALXN","ALGN",
    "ALLE","AGN","ADS","LNT","ALL","GOOGL","GOOG","MO","AMZN","AEE",
    "AAL","AEP","AXP","AIG","AMT","AWK","AMP","ABC","AME","AMGN",
    "APH","ADI","ANSS","ANTM","AON","APA","AAPL","AMAT","APTV","ADM",
    "ARNC","ANET","AJG","AIZ","T","ATO","ADSK","ADP","AZO","AVB",
    "AVY","BKR","BLL","BAC","BK","BAX","BDX","BRK.B","BBY","BIO",
    "BIIB","BLK","BKNG","BWA","BXP","BSX","BMY","AVGO","BR","BF.B",
    "CHRW","COG","CDNS","CPB","COF","CAH","KMX","CCL","CAT","CBOE",
    "CBRE","CDW","CE","CNC","CNP","CF","SCHW","CHTR","CVX","CMG",
    "CB","CHD","CI","CINF","CTAS","CSCO","C","CFG","CTXS","CLX",
    "CME","CMS","COO","COP","ED","STZ","CPRT","GLW","CTRA","COST",
    "CCI","CSX","CMI","CVS","DHI","DHR","DRI","DVA","DE","DAL",
    "XRAY","DVN","DXCM","FANG","DLR","DFS","DISCA","DISCK","DISH",
    "DG","DLTR","DOV","DTE","DRE","DUK","DXC","ETFC","EMN","ETN",
    "EBAY","EQT","EIX","EW","EA","EMR","ETR","ESS","EL","EOG",
    "ETSY","RE","EXC","EXPE","EXPD","EXR","XOM","FFIV","FBHS","FAST",
    "FRT","FDX","FIS","FITB","FRC","FE","FISV","FLT","FLIR","FLS",
    "FLR","FMC","F","FTNT","FTV","FB","FOXA","FOX","BEN","FCX",
    "GPS","GRMN","IT","GD","GEHC","GM","GPC","GILD","GL","GLPI",
    "GPN","GS","HAL","HBI","HRC","HIG","HAS","HCA","PEAK","HP",
    "HSIC","HSY","HES","HPE","HLT","HOLX","HD","HON","HRL","HPQ",
    "HST","HWM","HUM","HBAN","IEX","IDXX","INFO","ITW","ILMN","INCY",
    "IR","INTC","ICE","IFF","IP","IPG","IPGP","IQV","IRM","JKHY",
    "J","JBHT","SJM","JNJ","JCI","JPM","JNPR","KSU","K","KEY","KMB",
    "KIM","KMI","KLAC","KSS","KHC","KR","LHX","LH","LRCX","LW",
    "LVS","LEG","LDOS","LEN","LLY","LNC","LIN","LYV","LKQ","LMT",
    "L","LOW","LYB","MTB","MRO","MPC","MKTX","MAR","MMC","MLM",
    "MAS","MA","MKC","MXIM","MCD","MCK","MDT","MRK","MET","MTD",
    "MGM","MHK","MCHP","MU","MSFT","MSA","MSI","MS","MYL","NDAQ",
    "NOV","NTAP","NFLX","NWL","NEM","NWSA","NWS","NEE","NLSN","NIO",
    "NOC","NCLH","NRG","NUE","NVDA","NVR","NXPI","ORLY","OXY","ODFL",
    "OMC","OKE","ORCL","OGE","OTIS","PCAR","PKG","PH","PAYX","PYPL",
    "PNR","PEP","PKI","PFE","PM","PSX","PNW","PXD","PNC","POOL",
    "PPG","PPL","PFG","PG","PGR","PLD","PRU","PEG","PSA","PHM",
    "QRVO","PWR","QCOM","DGX","RL","RJF","RTX","O","REG","REGN",
    "RF","RSG","RMD","RHI","ROK","ROL","ROP","ROST","RCL","SPGI",
    "CRM","SBAC","SLB","STX","SEE","SRE","NOW","SHW","SPG","SWKS",
    "SLG","SNA","SO","LUV","SPY","SWK","SBUX","STT","STE","SYK",
    "SIVB","SYF","SNPS","SYY","TMUS","TROW","TPR","TRV","TRUE","TSCO",
    "TSN","TT","TDG","TRMB","TTC","TDY","TFX","TER","TSLA","TXN",
    "TXT","TMO","TJX","TSU","UHS","ULTA","USB","UDR","UAA","UA",
    "UNP","UAL","UNH","UPS","URI","UROV","VLO","VTR","VRSN","VRSK",
    "VZ","VRTX","VFC","VIAC","VTRS","VICI","V","VNO","VMC","WRB",
    "WMB","WLTW","WEC","WFC","WELL","WST","WM","WAT","WBA","DIS",
    "WMG","WEC","WY","WHR","WMB","WPM","WEC","WBD","VRTX"]

def add_row_id(df,interval):

    # TIMESTAMP datetime değilse çevir
    ts = pd.to_datetime(df['TIMESTAMP'])
    
    # time_id oluştur ve ROW_ID ata
    df['ROW_ID'] = (
        'ID_' +
        df['SYMBOL'].astype(str) +
        '_' +
        ts.dt.strftime('%Y%m%d_%H%M') +
        f'_{interval}')
    return df




def fetch_and_save(symbol,interval,source,START_DATE):
    print(f"\n>>> {symbol} başlatılıyor...")
    
    URL = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{START_DATE}/{datetime.now().date()}?adjusted=true&sort=asc&limit=50000&apiKey={API_KEY}"

    
    count = 0
    while URL:
        response = requests.get(URL)
        
        if response.status_code == 200:
            data = response.json()
            
            if "results" in data and data["results"]:
                df = pd.DataFrame(data["results"])
                
                # Polygon sütun isimlerini standart isimlere çevir
                # t: timestamp, o: open, h: high, l: low, c: close, v: volume
                df = df.rename(columns={
                    't': 'TIMESTAMP', 
                    'o': 'OPEN', 
                    'h': 'HIGH', 
                    'l': 'LOW', 
                    'c': 'CLOSE', 
                    'v': 'VOLUME'
                })
                
                # Zaman formatını düzelt
                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
                df['SYMBOL'] = symbol
                df['SOURCE'] = source
                df = add_row_id(df,interval)
                required_columns = ['SYMBOL','TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME','SOURCE','ROW_ID']
                df = df[required_columns]
                print(len(df))
                fn_distinct_write_to_db(df,'usa_1min_polygon_past','ROW_ID','append')

                count += len(df)
                # print(f"   [OK] {len(df)} satır eklendi. Toplam: {count}")
            
            if "next_url" in data:
                URL = data["next_url"] + f"&apiKey={API_KEY}"
                time.sleep(12.5) 
            else:
                URL = None
                
        elif response.status_code == 429:
            print("   [!] Limit aşıldı (429), 60 saniye bekleniyor...")
            time.sleep(60)
        else:
            print(f"   [X] Hata: {response.status_code}")
            break



for s in SYMBOLS:
    try:
        fetch_and_save(s,interval = '1min',source='polygon',START_DATE=START_DATE)
    except Exception as e:
        err_dic = {'SYMBOL':s,
                  'ERROR':e}

        df_err = pd.DataFrame([err_dic])
        df_err.to_sql('error_list_usa_polygon_1min', engine_local, if_exists='append', index=False)
        print(f"Hata oluştu ({s}): {e}")
        continue
print("\n--- TÜM İŞLEMLER TAMAMLANDI ---")

