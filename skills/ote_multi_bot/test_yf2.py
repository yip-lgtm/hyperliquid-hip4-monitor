import yfinance as yf
import time
import sys

print('yfinance version:', yf.__version__)

# Wait a bit before making requests
time.sleep(3)

symbols = ['AAPL', 'BTC-USD', 'MNQ=F']
for sym in symbols:
    try:
        print(f'\nTrying {sym}...')
        t = yf.Ticker(sym)
        h = t.history(period='1d', interval='5m')
        print(f'{sym}: {len(h)} bars received')
        if len(h) > 0:
            print(f'  Last close: {h["Close"].iloc[-1]:.2f}')
            print(f'  Columns: {h.columns.tolist()}')
    except Exception as e:
        print(f'{sym}: {type(e).__name__}: {str(e)[:120]}')
    time.sleep(2)