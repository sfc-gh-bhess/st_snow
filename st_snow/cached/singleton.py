import snowflake.connector
from snowflake.snowpark import Session
import streamlit as st
from ..singleton import _SnowflakeConnectionWrapper, _SnowflakeSessionWrapper
from . import cached

class _SnowcacheConnectionWrapper(_SnowflakeConnectionWrapper):
    def __init__(self):
        super().__init__()

    def _create_connection(self, *args, **kwargs) -> snowflake.connector.SnowflakeConnection:
        return cached.SnowCacheConnection(*args, **kwargs)

def connection(*args, **kwargs) -> snowflake.connector.SnowflakeConnection:
    @st.experimental_singleton
    def get_connection(*args, **kwargs) -> _SnowflakeConnectionWrapper:
        return _SnowcacheConnectionWrapper()
    
    return get_connection(*args, **kwargs).get_connection(*args, **kwargs)

class _SnowcacheSessionWrapper(_SnowflakeSessionWrapper):
    def __init__(self):
        super().__init__()
        
    def _create_connection(self, *args, **kwargs) -> Session:
        return cached.session_builder.configs(*args, **kwargs).create()

def session(*args, **kwargs) -> Session:
    @st.experimental_singleton
    def get_connection(*args, **kwargs) -> _SnowflakeSessionWrapper:
        return _SnowcacheSessionWrapper()
    
    return get_connection(*args, **kwargs).get_connection(*args, **kwargs)
