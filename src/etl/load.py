# %%
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Float, Date, Integer, UniqueConstraint, text
from sqlalchemy.orm import declarative_base

# %%
Base = declarative_base()

class Commodity(Base):
    __tablename__ = 'commodity'
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    currency = Column(String, nullable=False)
    extraction_date = Column(Date, nullable=False)
    asset_name = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint('asset', 'extraction_date', name='uix_asset_extraction_date'),
    )

# %%
engine = create_engine('sqlite:///./data/database/warehouse.db', echo=True)
Base.metadata.create_all(engine)

# %%
# Load parquet
input_file_name = f'commodities_data_{datetime.now().strftime("%d-%m-%Y").replace("-", "_")}.parquet'
input_file_path = f'./data/parquet/{input_file_name}'
df = pd.read_parquet(input_file_path, engine="fastparquet")

df['extraction_date'] = pd.to_datetime(df['extraction_date']).dt.date  # truncate any time

# %%
# Load existing entries to avoid duplicates
existing_df = pd.read_sql(text("SELECT asset, extraction_date FROM commodity"), engine)
existing_df['extraction_date'] = pd.to_datetime(existing_df['extraction_date']).dt.date

# Keep only new rows
df = df.merge(existing_df, on=['asset', 'extraction_date'], how='left', indicator=True)
df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

# %%
# Insert new rows
if not df.empty:
    df.to_sql('commodity', engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} new rows.")
else:
    print("No new rows to insert — everything is already in the database.")