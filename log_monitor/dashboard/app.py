import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import streamlit as st
import pandas as pd
import plotly.express as px
from src.parser import parse_logs
from src.detector import detect_anomalies

st.set_page_config(page_title="Log Monitor - ABB", layout="wide")
st.title("Log Monitor - Virements Instantanes ABB")
st.caption("Analyse des logs integration systeme webMethods")

uploaded = st.file_uploader("Charger un fichier de logs", type=['txt', 'log', '1'])

if uploaded:
    os.makedirs("data", exist_ok=True)
    with open("data/temp.log", "wb") as f:
        f.write(uploaded.read())
    df = parse_logs("data/temp.log")
else:
    try:
        import glob
        logs = glob.glob('data/*')
        df = parse_logs(logs[0])
        st.info("Fichier charge automatiquement : " + logs[0])
    except:
        st.warning("Aucun fichier de logs trouve.")
        st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total lignes", len(df))
col2.metric("Erreurs", len(df[df['severity'] == 'ERROR']))
col3.metric("Infos", len(df[df['severity'] == 'INFO']))
col4.metric("Services touches", df['service'].nunique())

st.divider()

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Erreurs par code")
    freq = df['error_code'].value_counts().reset_index()
    freq.columns = ['error_code', 'count']
    fig1 = px.bar(freq.head(10), x='error_code', y='count', color='count', color_continuous_scale='Reds')
    st.plotly_chart(fig1, use_container_width=True)

with col_b:
    st.subheader("Erreurs par service")
    fig2 = px.pie(df, names='service')
    st.plotly_chart(fig2, use_container_width=True)

st.subheader("Timeline des erreurs par heure")
df['hour'] = pd.to_datetime(df['timestamp']).dt.floor('h')
timeline = df.groupby(['hour', 'severity']).size().reset_index(name='count')
fig3 = px.line(timeline, x='hour', y='count', color='severity',
               color_discrete_map={'ERROR': 'red', 'WARNING': 'orange', 'INFO': 'blue'})
st.plotly_chart(fig3, use_container_width=True)

st.subheader("Alertes detectees")
alerts = detect_anomalies(df)
for _, row in alerts.iterrows():
    if row['severity'] == 'CRITICAL':
        st.error(f"[CRITIQUE] {row['type']} - {row['detail']}")
    elif row['severity'] == 'ERROR':
        st.warning(f"[ERREUR] {row['type']} - {row['detail']}")
    else:
        st.info(f"[WARNING] {row['type']} - {row['detail']}")

st.subheader("Detail des logs")
st.dataframe(df, use_container_width=True)
st.download_button("Telecharger CSV", df.to_csv(index=False), "errors_report.csv")