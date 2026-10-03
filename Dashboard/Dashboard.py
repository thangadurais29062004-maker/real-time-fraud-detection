import json

import altair as alt
import pandas as pd
import psycopg2
import redis
import streamlit as st

st.set_page_config(page_title="Fraud Monitor", layout="wide")
st.title("Real-Time Fraud Detection Dashboard")

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

ORDER = ["ALLOW", "ALERT", "BLOCK"]
COLORS = ["#2ca02c", "#ff9f1c", "#d62728"]
BINS = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]


def query(sql):
    conn = psycopg2.connect(host="localhost", port=5432, dbname="fraud_db",
                            user="fraud", password="fraud123")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


@st.fragment(run_every=3)
def live():
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total transactions", r.get("stats:total") or 0)
    c2.metric("Allowed", r.get("stats:ALLOW") or 0)
    c3.metric("Alerts", r.get("stats:ALERT") or 0)
    c4.metric("Blocked", r.get("stats:BLOCK") or 0)

    left, right = st.columns(2)

    with left:
        st.subheader("Decisions")
        counts = query("SELECT decision, COUNT(*) AS n FROM predictions GROUP BY decision")
        if not counts.empty:
            chart = alt.Chart(counts).mark_bar().encode(
                x=alt.X("decision:N", sort=ORDER, title=None),
                y=alt.Y("n:Q", scale=alt.Scale(zero=True), title="Transactions"),
                color=alt.Color("decision:N",
                                scale=alt.Scale(domain=ORDER, range=COLORS),
                                legend=None),
            )
            st.altair_chart(chart, width="stretch")

        st.subheader("Risk score distribution")
        scores = query("SELECT risk_score FROM predictions")
        if not scores.empty:
            cut = pd.cut(scores["risk_score"], bins=BINS, include_lowest=True)
            hist = cut.value_counts().sort_index().reset_index()
            hist.columns = ["range", "n"]
            hist["range"] = hist["range"].astype(str)
            hist_chart = alt.Chart(hist).mark_bar().encode(
                x=alt.X("range:N", sort=list(hist["range"]), title="Risk score"),
                y=alt.Y("n:Q", scale=alt.Scale(zero=True), title="Transactions"),
            )
            st.altair_chart(hist_chart, width="stretch")

    with right:
        st.subheader("Latest alerts and blocks (Redis)")
        alerts = [json.loads(a) for a in r.lrange("recent_alerts", 0, 14)]
        if alerts:
            st.dataframe(pd.DataFrame(alerts), width="stretch", hide_index=True)
        else:
            st.info("No alerts yet.")

        st.subheader("Predictions vs real labels")
        check = query("""SELECT decision, COUNT(*) AS transactions,
                                SUM(actual_is_fraud) AS real_frauds
                         FROM predictions GROUP BY decision ORDER BY decision""")
        st.dataframe(check, width="stretch", hide_index=True)

    st.subheader("Latest transactions with real-time Spark features (PostgreSQL)")
    latest = query("""SELECT created_at, user_id, amount, location, risk_score, decision,
                             tx_count_5m, avg_amount_5m, distinct_locations_5m,
                             secs_since_last_tx
                      FROM predictions ORDER BY id DESC LIMIT 20""")
    st.dataframe(latest, width="stretch", hide_index=True)


live()