import streamlit as st 
import snowflake.connector
from snowflake.snowpark import Session

class _SnowflakeConnectionWrapper:
    def __init__(self):
        self._connection = None
    def get_connection(self, *args, **kwargs) -> snowflake.connector.SnowflakeConnection:
        if not self._validate_connection():
            self._connection = self._create_connection(*args, **kwargs)
        return self._connection
    def _validate_connection(self) -> bool:
        if self._connection is None:
            return False
        if self._connection.is_closed():
            return False
        return True
    def _create_connection(self, *args, **kwargs) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(*args, **kwargs)

def connection(*args, **kwargs) -> snowflake.connector.SnowflakeConnection:
    @st.experimental_singleton
    def get_connection(*args, **kwargs) -> _SnowflakeConnectionWrapper:
        return _SnowflakeConnectionWrapper()
    
    return get_connection(*args, **kwargs).get_connection(*args, **kwargs)

class _SnowflakeSessionWrapper:
    def __init__(self):
        self._connection = None
    def get_connection(self, *args, **kwargs) -> Session:
        if not self._validate_connection():
            self._connection = self._create_connection(*args, **kwargs)
        return self._connection
    def _validate_connection(self) -> bool:
        if self._connection is None:
            return False
        if self._connection._conn._conn.is_closed():
            return False
        return True
    def _create_connection(self, *args, **kwargs) -> Session:
        return Session.builder.configs(*args, **kwargs).create()

def session(*args, **kwargs) -> Session:
    @st.experimental_singleton
    def get_connection(*args, **kwargs) -> _SnowflakeSessionWrapper:
        return _SnowflakeSessionWrapper()
    
    return get_connection(*args, **kwargs).get_connection(*args, **kwargs)
