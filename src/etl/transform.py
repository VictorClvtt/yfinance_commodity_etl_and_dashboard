# %%
import pandas as pd
from datetime import datetime

# %%
def transform_commodities_df(df: pd.DataFrame) -> pd.DataFrame:
    # Map asset names
    asset_map = {
        "GC=F": "Gold",
        "CL=F": "Crude Oil",
        "SI=F": "Silver",
        "PL=F": "Platinum",
        "HG=F": "Copper",
        "NG=F": "Natural Gas",
        "ZC=F": "Corn",
        "ZS=F": "Soybeans",
        "KC=F": "Coffee",
        "CT=F": "Cotton",
        "^GSPC": "S&P 500",
        "^DJI": "Dow Jones",
        "^IXIC": "Nasdaq"
    }
    df["asset_name"] = df["asset"].map(asset_map)

    # Ensure correct dtypes
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["collection_time"] = pd.to_datetime(df["collection_time"], utc=True)

    # Add category column
    df["category"] = df["asset_name"].apply(
        lambda x: "Index" if x in ["S&P 500", "Dow Jones", "Nasdaq"] else "Commodity"
    )

    # Drop NaNs
    df = df.dropna(subset=["price"])

    return df

# %%
input_file_name = f'commodities_data_{datetime.now().strftime("%d-%m-%Y").replace('-', '_')}.csv'
input_file_path = f'./data/csv/{input_file_name}'

df = pd.read_csv(input_file_path)

df = transform_commodities_df(df)
df.dtypes
# %%
output_file_name = f'commodities_data_{datetime.now().strftime("%d-%m-%Y").replace('-', '_')}.parquet'
output_file_path = f'./data/parquet/{output_file_name}'

df.to_parquet(output_file_path)