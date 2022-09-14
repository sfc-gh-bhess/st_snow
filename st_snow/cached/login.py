import snowflake.connector
from snowflake.snowpark import Session
import streamlit as st
from typing import Dict, Union
from ..login import _snowflakeLoginForm, _session_api as login_session_api, _connection_api as login_connection_api
from . import cached

class _session_api(login_session_api):
    def __init__(self, ttl=None):
        super().__init__()
        self.ttl = ttl

    def connect(self, c) -> Session:
        if self.ttl:
            c[cached.SnowCacheCursor.CACHEKEY_TTL] = self.ttl
        session = cached.session_builder.configs(c).create()
        st.session_state[self.STKEY] = session
        return session

class _connection_api(login_connection_api):
    def __init__(self, ttl=None):
        super().__init__()
        self.ttl = ttl

    def connect(self, c) -> snowflake.connector.SnowflakeConnection:
        if self.ttl:
            c[cached.SnowCacheCursor.CACHEKEY_TTL] = self.ttl
        connection = cached.SnowCacheConnection(**c)
        st.session_state[self.STKEY] = connection
        return connection

def session(form_options: Dict[str, Union[str,int]] = None, options: Dict[str, Union[str,int]] = None) -> Session:
    if not options:
        options = {'ttl': 3600}
    c = _session_api(options['ttl'] if 'ttl' in options else None)
    return _snowflakeLoginForm(c, form_options, options)

def connection(form_options: Dict[str, Union[str,int]] = None, options: Dict[str, Union[str,int]] = None) -> snowflake.connector.SnowflakeConnection:
    if not options:
        options = {'ttl': 3600}
    c = _connection_api(options['ttl'] if 'ttl' in options else None)
    return _snowflakeLoginForm(c, form_options, options)