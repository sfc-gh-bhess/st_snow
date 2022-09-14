import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import snowflake.snowpark.functions as f
import json
import datetime
import snowflake.connector

import sys
sys.path.append('../src')
import st_snow

def getOrRegetSession(fname = "/tmp/snowflake/creds.json") -> Session:
    snowcreds = json.load(open(fname, "r"))
    snowcreds["database"] = "citibike"
    snowcreds["schema"] = "demo"
    return st_snow.singleton.session(snowcreds)

def getOrRegetConnection(fname = "/tmp/snowflake/creds.json") -> snowflake.connector.SnowflakeConnection:
    snowcreds = json.load(open(fname, "r"))
    snowcreds["database"] = "citibike"
    snowcreds["schema"] = "demo"
    return st_snow.singleton.connection(**snowcreds)

conn = getOrRegetConnection()
st.markdown(f"Connection Session ID: {conn.session_id}")
st.markdown(f"Connection Account: {conn.account}")
st.button("Close connection", on_click=lambda c: c.close(), args=(conn,))

st.markdown("## Hiya")
session = getOrRegetSession()
st.markdown(f"Session ID: {session._conn._conn.session_id}")
st.markdown(f"Connection Account: {session._conn._conn.account}")
st.button("Close Session", on_click=lambda s: s.close(), args=(session,))

st.markdown('---')
conn2 = getOrRegetConnection()
st.markdown(f"Connection Session ID: {conn2.session_id}")
st.markdown(f"Connection Account: {conn2.account}")
st.button("Close connection2", on_click=lambda c: c.close(), args=(conn2,))

session2 = getOrRegetSession()
st.markdown(f"Session ID: {session2._conn._conn.session_id}")
st.markdown(f"Connection Account: {session2._conn._conn.account}")
st.button("Close Session2", on_click=lambda s: s.close(), args=(session2,))

st.markdown('---')
conn3 = getOrRegetConnection('/Users/bhess/snowcreds/demo-nac-stuser.json')
st.markdown(f"Connection Session ID: {conn3.session_id}")
st.markdown(f"Connection Account: {conn3.account}")
st.button("Close connection3", on_click=lambda c: c.close(), args=(conn3,))

session3 = getOrRegetSession('/Users/bhess/snowcreds/demo-nac-stuser.json')
st.markdown(f"Session ID: {session3._conn._conn.session_id}")
st.markdown(f"Connection Account: {session3._conn._conn.account}")
st.button("Close Session3", on_click=lambda s: s.close(), args=(session3,))


'''
____

## Citibike 1
'''
def executeStartGroupBy(start_start, start_end):
    cb_sql1 = f"SELECT COUNT(*) AS ct, start_borough FROM citibike.demo.trips_stations_vw WHERE starttime >= '{start_start.isoformat()}' AND starttime <= '{start_end.isoformat()}' GROUP BY start_borough ORDER BY ct DESC"
    return session.table("trips_stations_vw") \
        .filter((col("starttime") >= start_start.isoformat()) & (col("starttime") <= start_end.isoformat())) \
        .group_by(col("start_borough")) \
        .agg(f.count("start_borough").alias("ct")).to_pandas()

start_start = st.date_input("What is the start date", datetime.date(2016,8,11), key="start_start")
start_end  =  st.date_input("What is the end date", datetime.date(2016,8,18), key="start_end")
cb_df1 = executeStartGroupBy(start_start, start_end)
st.text(f"From {start_start.isoformat()} to {start_end.isoformat()}")
cb_df1

'''
## Citibike 2
'''
def executeEndGroupBy(end_start, end_end):
    cb_sql2 = f"SELECT COUNT(*) AS ct, end_borough FROM citibike.demo.trips_stations_vw WHERE endtime >= '{end_start.isoformat()}' AND endtime <= '{end_end.isoformat()}' GROUP BY end_borough ORDER BY ct DESC"
    return session.table("trips_stations_vw") \
        .filter((col("endtime") >= end_start.isoformat()) & (col("endtime") <= end_end.isoformat())) \
        .group_by(col("end_borough")) \
        .agg(f.count("end_borough").alias("ct")).to_pandas()


end_start = st.date_input("What is the start date", datetime.date(2016,8,11), key="end_start")
end_end  =  st.date_input("What is the end date", datetime.date(2016,8,18), key="end_end")
cb_df2 = executeEndGroupBy(end_start, end_end)
st.text(f"From {end_start.isoformat()} to {end_end.isoformat()}")
cb_df2



