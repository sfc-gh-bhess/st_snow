import streamlit as st 
import snowflake.connector
from snowflake.snowpark import Session
from typing import Union, Dict

class _session_api: 
    def __init__(self):
        self.STKEY = 'STSNOW_SESSION'

    def is_open(self, s: Session) -> bool:
        return not s._conn._conn.is_closed()

    def connect(self, c: Dict[str, Union[str,int]]) -> Session:
        session = Session.builder.configs(c).create()
        st.session_state[self.STKEY] = session
        return session

    def close(self):
        st.session_state[self.STKEY].close()
        del st.session_state[self.STKEY]
    
class _connection_api: 
    def __init__(self):
        self.STKEY = 'STSNOW_CONNECTION'

    def is_open(self, c: snowflake.connector.SnowflakeConnection) -> bool:
        return not c.is_closed()

    def connect(self, c: Dict[str, Union[str,int]]) -> snowflake.connector.SnowflakeConnection:
        connection = snowflake.connector.connect(**c)
        st.session_state[self.STKEY] = connection
        return connection

    def close(self):
        st.session_state[self.STKEY].close()
        del st.session_state[self.STKEY]

def _callbackAndClear(callback, prefix, options) -> None:
    stcreds = {key:val for key,val in st.session_state.items() if key.startswith(prefix)}
    for k,v in stcreds.items():
        if v != "":
            options[k[len(prefix):]] = v
        del st.session_state[k]
    callback(options)

STSNOW_CREDENTIALS = 'STSNOW_CREDENTIALS_'
def _snowflakeLoginForm(impl: Union[_session_api, _connection_api], 
                        form_options: Dict[str, Union[int,str]],
                        options: Dict[str, Union[int,str]]) -> Union[Session, snowflake.connector.SnowflakeConnection]:
    if impl.STKEY in st.session_state:
        if impl.is_open(st.session_state[impl.STKEY]):
            st.sidebar.button("Disconnect from Snowflake", on_click=impl.close)
            return st.session_state[impl.STKEY]
        else:
            del st.session_state[impl.STKEY]
    if not form_options:
        form_options = {'account':'', 'user':'', 'password':''}
    if not options:
        options = {}
    with st.form("Snowflake Credentials"):
        st.subheader("Snowflake Credentials")
        # Must have account, user, password
        if "account" not in options:
            st.text_input("Account", value=form_options["account"] if "account" in form_options else "", key=f"{STSNOW_CREDENTIALS}account")
        if "user" not in options:
            st.text_input("User", value=form_options["user"] if "user" in form_options else "", key=f"{STSNOW_CREDENTIALS}user")
        if "password" not in options:
            st.text_input("Password", type="password", value=form_options["password"] if "password" in form_options else "", key=f"{STSNOW_CREDENTIALS}password")

        # Generate form for other options
        for k,v in {key:val for key,val in form_options.items() if key not in ["account", "user", "password"]}.items():
            st.text_input(k.capitalize(), value=v, key=f"{STSNOW_CREDENTIALS}{k}")
        st.form_submit_button("Connect", on_click=_callbackAndClear, args=(impl.connect, STSNOW_CREDENTIALS, options),)
    st.stop()

def session(form_options: Dict[str, Union[str,int]] = None, options: Dict[str, Union[str,int]] = None) -> Session:
    c = _session_api()
    return _snowflakeLoginForm(c, form_options, options)

def connection(form_options: Dict[str, Union[str,int]] = None, options: Dict[str, Union[str,int]] = None) -> snowflake.connector.SnowflakeConnection:
    c = _connection_api()
    return _snowflakeLoginForm(c, form_options, options)

