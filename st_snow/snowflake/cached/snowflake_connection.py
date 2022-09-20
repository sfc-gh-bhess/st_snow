from cached.cached import SnowCacheConnection, session_builder
import streamlit as st
from st_snow import snowflake_connection
import snowflake.connector
from snowflake.snowpark import Session

class SnowflakeCachedConnectionImpl(snowflake_connection.SnowflakeConnection):
    def connect(self, params) -> snowflake.connector.SnowflakeConnection:
        return SnowCacheConnection(**params)

class SnowflakeCachedSessionImpl(snowflake_connection.SnowflakeSession):
    def connect(self, params) -> Session:
        return session_builder.configs(params).create()


class cached:
    connection = st.connection.connection(SnowflakeCachedConnectionImpl())
    session = st.connection.connection(SnowflakeCachedSessionImpl())

# Placing in st.connection for convenience
# Instantiate a connection with:
#    conn = st.connection.snowflake.cached.connection.login()
# or
#    conn = st.connection.snowflake.cached.connection.singleton()
# Instantiate a session with:
#    session = st.connection.snowflake.cached.session.login()
# or
#    session = st.connection.snowflake.cached.session.singleton()
st.connection.snowflake.cached = cached
