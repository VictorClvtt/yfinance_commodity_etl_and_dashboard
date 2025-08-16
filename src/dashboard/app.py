# %%
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# %%
engine = create_engine('sqlite:///./data/database/warehouse.db', echo=True)
df = pd.read_sql(text("SELECT * FROM commodity"), engine)

# %%
st.title('📊 Commodities')
df

# %%

st.subheader('💡 Main KPIs')
col1, col2, col3 = st.columns(3)

total_itens = df.shape[0]
col1.metric(label="🪙 Total Assets", value=total_itens)

