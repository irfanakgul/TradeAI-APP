import yfrlt
import time
import signal
import sys

print("connecting......")

running = True

def shutdown(signum=None, frame=None):
    global running
    print("\nProgram manuel olarak kapatılıyor...")
    running = False
    try:
        client.stop()   # WebSocket'i durdur
    except:
        pass
    sys.exit(0)

def on_price_update(data):
    print(f"{data.symbol}: ${data.price:.2f} ({data.change_percent:+.2f}%)")

client = yfrlt.Client()
client.subscribe(['BTC-USD'], on_price_update)

print("Abone olundu, fiyat güncellemeleri bekleniyor...")

# CTRL + C ile kapatma
signal.signal(signal.SIGINT, shutdown)

client.start()

# Sonsuz döngü, manuel kapanana kadar devam eder
while running:
    time.sleep(1)
    