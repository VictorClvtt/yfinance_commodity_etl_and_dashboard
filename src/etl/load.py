# %%
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, UniqueConstraint, text
from sqlalchemy.orm import declarative_base

# %%
Base = declarative_base()

class Commodity(Base):
    __tablename__ = 'commodity'
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    currency = Column(String, nullable=False)
    collection_time = Column(DateTime, nullable=False)
    asset_name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    _datetime = Column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint('asset', 'collection_time', name='uix_asset_collection_time'),
    )

engine = create_engine('sqlite:///./data/database/warehouse.db', echo=True)
Base.metadata.create_all(engine)

# %%
input_file_name = f'commodities_data_{datetime.now().strftime("%d-%m-%Y").replace("-", "_")}.parquet'
input_file_path = f'./data/parquet/{input_file_name}'

df = pd.read_parquet(input_file_path, engine="fastparquet")

df['_datetime'] = datetime.now().strftime("%d-%m-%Y")

# Ensure datetime format matches DB expectations
df['_datetime'] = pd.to_datetime(df['_datetime'])

# %%
existing_df = pd.read_sql(text("SELECT asset, _datetime FROM commodity"), engine)
existing_df['_datetime'] = pd.to_datetime(existing_df['_datetime'])

# Keep only new rows
df = df.merge(existing_df, on=['asset', '_datetime'], how='left', indicator=True)
df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

# %%
if not df.empty:
    df.to_sql('commodity', engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} new rows.")
else:
    print("No new rows to insert — everything is already in the database.")