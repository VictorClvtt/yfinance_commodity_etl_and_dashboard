# %%
import pandas as pd
import yfinance as yf
from datetime import datetime

# %%
def get_commodities_df(symbols: list) -> pd.DataFrame:
    dfs = []
    for sym in symbols:
        latest_df = yf.Ticker(sym).history(period='1d', interval='1m')[['Close']].tail(1)
        latest_df = latest_df.rename(columns={'Close': 'price'})
        latest_df['asset'] = sym
        latest_df['currency'] = 'USD'
        latest_df['collection_time'] = datetime.now()
        latest_df = latest_df[['asset', 'price', 'currency', 'collection_time']]
        dfs.append(latest_df)
    return pd.concat(dfs, ignore_index=True)

symbols = [
        "GC=F",   # Gold
        "CL=F",   # Crude Oil
        "SI=F",   # Silver
        "PL=F",   # Platinum
        "HG=F",   # Copper
        "NG=F",   # Natural Gas
        "ZC=F",   # Corn
        "ZS=F",   # Soybeans
        "KC=F",   # Coffee
        "CT=F",   # Cotton
        "^GSPC",  # S&P 500 Index
        "^DJI",   # Dow Jones
        "^IXIC"   # Nasdaq
    ]
df = get_commodities_df(symbols)

# %%
file_name = f'commodities_data_{datetime.now().strftime("%d-%m-%Y").replace('-', '_')}.csv'

df.to_csv(f'./data/csv/{file_name}', index=False)