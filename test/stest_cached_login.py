import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import snowflake.snowpark.functions as f
import datetime

#import sys
#sys.path.append('../src')
import st_snow.cached

conntype = st.selectbox("Session or Cnonection", options=['Session', 'Connection'])

st.markdown("## Hiya")

if 'Session' == conntype:
    # session = st_snow.cached.login.session({'account':'xwa28048', 'user':'bmhess', 'warehouse':'load_wh', 'database':'citibike', 'schema':'demo'}, {'ttl':60})
    session = st_snow.cached.login.session()
    session.use_warehouse('load_wh')
    st.markdown(f"Session ID: {session._conn._conn.session_id}")
    st.button("Force close", on_click=lambda s: s.close(), args=(session,))

if 'Connection' == conntype:
    # conn = st_snow.cached.login.connection({'account':'xwa28048', 'user':'bmhess', 'warehouse':'load_wh', 'database':'citibike', 'schema':'demo'}, {'ttl':60})
    conn = st_snow.cached.login.connection()
    conn.cursor().execute('USE WAREHOUSE load_wh')
    st.markdown(f"Session ID: {conn.session_id}")
    st.button("Force close", on_click=lambda s: s.close(), args=(conn,))


'''
____

## Citibike 1
'''
def executeStartGroupBy(start_start, start_end):
    if 'Connection' == conntype:
        cb_sql1 = f"SELECT COUNT(*) AS ct, start_borough FROM citibike.demo.trips_stations_vw WHERE starttime >= '{start_start.isoformat()}' AND starttime <= '{start_end.isoformat()}' GROUP BY start_borough ORDER BY ct DESC"
        return conn.cursor().execute(cb_sql1).fetch_pandas_all()
    return session.table("citibike.demo.trips_stations_vw") \
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
    if 'Connection' == conntype:
        cb_sql2 = f"SELECT COUNT(*) AS ct, end_borough FROM citibike.demo.trips_stations_vw WHERE endtime >= '{end_start.isoformat()}' AND endtime <= '{end_end.isoformat()}' GROUP BY end_borough ORDER BY ct DESC"
        return conn.cursor().execute(cb_sql2).fetch_pandas_all()
    return session.table("citibike.demo.trips_stations_vw") \
        .filter((col("endtime") >= end_start.isoformat()) & (col("endtime") <= end_end.isoformat())) \
        .group_by(col("end_borough")) \
        .agg(f.count("end_borough").alias("ct")).to_pandas()


end_start = st.date_input("What is the start date", datetime.date(2016,8,11), key="end_start")
end_end  =  st.date_input("What is the end date", datetime.date(2016,8,18), key="end_end")
cb_df2 = executeEndGroupBy(end_start, end_end)
st.text(f"From {end_start.isoformat()} to {end_end.isoformat()}")
cb_df2




st.sidebar.json(st.session_state)

