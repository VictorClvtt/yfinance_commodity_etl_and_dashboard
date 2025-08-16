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

df

# KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Ativos únicos", num_assets)
col2.metric("Preço médio (USD)", f"{mean_price:,.2f}")
col3.metric("Ativo mais caro", f"{max_asset['asset_name']} ({max_asset['price']:,.2f} USD)")

st.markdown("---")

# --- Gráfico de barras ---
st.subheader("Preço por Ativo")
st.bar_chart(df.set_index("asset_name")["price"])

# --- Gráfico de linha (se houver várias datas) ---
if df['extraction_date'].nunique() > 1:
    st.subheader("Evolução dos preços")
    df_line = df.groupby(["extraction_date", "asset_name"])['price'].mean().reset_index()
    line_chart_data = df_line.pivot(index="extraction_date", columns="asset_name", values="price")
    st.line_chart(line_chart_data)

# %%
