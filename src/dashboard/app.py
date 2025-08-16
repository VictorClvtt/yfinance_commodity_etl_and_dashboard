import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


engine = create_engine('sqlite:///./data/database/warehouse.db', echo=True)
df = pd.read_sql(text("SELECT * FROM commodity"), engine)

# --- KPIs ---
num_assets = df['asset'].nunique()
mean_price = df['price'].mean()
max_asset = df.loc[df['price'].idxmax()]


st.title('📊 Commodities Dashboard')

df.drop(columns=["id"], inplace=True)
df

# KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Unique assets:", num_assets)
col2.metric("Mean price (USD):", f"{mean_price:,.2f}")
col3.metric("Most expensive asset:", f"{max_asset['asset_name']} ({max_asset['price']:,.2f} USD)")

st.markdown("---")

# --- Gráfico de barras ---
st.subheader("Price by Asset(most recent values):")

# pegar o dia mais atual
latest_date = df["extraction_date"].max()
df_latest = df[df["extraction_date"] == latest_date]

# gráfico de barras apenas do último dia
st.bar_chart(df_latest.set_index("asset_name")["price"])

# --- Gráfico de linha (se houver várias datas) ---
if df['extraction_date'].nunique() > 1:
    st.subheader("Assets price evolution:")
    df_line = df.groupby(["extraction_date", "asset_name"])['price'].mean().reset_index()
    line_chart_data = df_line.pivot(index="extraction_date", columns="asset_name", values="price")
    st.line_chart(line_chart_data)