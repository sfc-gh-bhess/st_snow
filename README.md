# Snowflake Connection Tools for Streamlit

This Python package helps users connect to Snowflake from Streamlit.
It provides a number of approaches for various use cases, and has some
best-practices built in.

## Description

Streamlit is a powerful data and visualization tool, and Snowflake is a
powerful data storage and processing engine. Together... they're Grrrrreat!

There are a few different scenarios about how a Streamlit app will connect
to a data source (such as Snowflake):
1. A single, global connection to be used by all Streamlit users
2. Each Streamlit user provides their own Snowflake credentials and the
Snowflake session is unique to them.

This package provides both ways to connect, with some specific nuances for 
each case.
1. For a global connection, the credentials will be static (e.g., in a credentials
file, or in the `st.secrets` construct). As such, we can automatically connect,
and if there's a need to reconnect, we have the credentials and can do that
automatically, as well. In this way, the user will never error by using a stale/closed
connection.
2. For the case where users provide their own credentials, we would want to 
present a login form to capture the necessary details to connect to Snowflake
(e.g., account, username, password, etc) and not present any more of the 
Streamlit app until those details were entered. Once the details are entered,
the connection to Snowflake should be made, and the credentials should be 
deleted (so as to minimize any security risk). While connected, there should 
be a logout button to end the session. However, it may come to pass that the 
connection may get closed (e.g., a failure, an idle timeout, etc). In that case,
we would want to act as if the user never logged in, namely we want to present
the login screen and gate the app until successfully logged in.

There are also 2 different types of connections to Snowflake (today):
1. The Snowflake Python Connector (a `SnowflakeConnection`)
2. The Snowflake Snowpark Python library (a `SnowparkSession`)

This package provides functions to support both scenarios above with each
of these types of connections.

## Singleton Connections
To support making a single global connection to Snowflake we can use the
`st_snow.singleton` package and the `connection()` and/or `session()` methods.

These connections will be shared across all Streamlit sessions. They will
test if the connection is closed (e.g., due to inactivity), and if so
they will automatically reconnect, so the users will never use a stale/closed
connection.

### Snowflake Python Connector
To connect to Snowflake using the Snowflake Python Connector, instead of 
using `snowflake.connector.connect()`, you would use `st_snow.singleton.connect()`:

```
import json
import st_snow

def connect_to_snowflake(fname):
    snowcreds = json.load(open(fname, "r"))
    return st_snow.singleton.connection(**snowcreds)

conn = connect_to_snowflake("/path/to/json/credentials.json")
```

At that point you can use the connection just like you would otherwise. If
the connection gets severed, it will automatically be recreated.

You can pass any parameters to `st_snow.singleton.connection()` that you would
normally pass to `snowflake.connector.connect()`.

### Snowpark Python
To connect to Snowflake using the Snowpark Python package, instead of 
using `snowflake.snowpark.Session.builder`, you would use `st_snow.singleton.session()`:

```
import json
import st_snow

def connect_to_snowflake(fname):
    snowcreds = json.load(open(fname, "r"))
    return st_snow.singleton.session(snowcreds)

session = connect_to_snowflake("/path/to/json/credentials.json")
```

At that point you can use the session just like you would otherwise. If
the connection gets severed, the session will automatically be recreated.

You can pass any parameters to `st_snow.singleton.session()` that you would
normally pass to `snowflake.snowpark.Session.builder.configs()`.


## Login Form
To support making a connection per user to Snowflake we can use the
`st_snow.login` package and the `connection()` and/or `session()` methods.

These connections will not be shared across Streamlit sessions, but will
be unique to each Streamlit user. They will present a login form, and 
once the details have been entered, a connection will be made. That connection
will be cached in the user's session state for efficient use.

These connections  will test if the connection is closed (e.g., due to inactivity), 
and if so they will revert to the login screen to get the credentials from the 
user and reconnect. As such,  the users will never use a stale/closed 
connection.

Once connected, a logout button is placed in the sidebar.

### Snowflake Python Connector
To connect to Snowflake using the Snowflake Python Connector with a login form,
you would use `st_snow.login.connect()`:

```
import st_snow

## Things above here will be run before (and after) you log in

conn = st_snow.login.connection()

## Nothing below here will be run until you log in
```

At that point you can use the connection just like you would otherwise. If
the connection gets severed, the login form will be presented again.

The login function takes 2 arguments:
* `form_options`: a dictionary. The keys of this dictionary will become input
fields for the user to fill in (e.g, `user`). The values of the keys will be
the default value (leave `""` for no default value).
* `options`: a dictionary. These options will be passed as-is to the connection.
For example, if you wanted to set a `timezone` and not allow a user to edit that,
you could put it in `options`. If you wanted them to edit it, you would put it
in `form_options`.

For example:

```
import st_snow

conn = st_snow.login.connection({'database': 'PROJECT_DB', 'schema':''}, {'account': 'XXX', 'warehouse': 'PROJECT_WH'})
```

This would create a form to collect the `account`, `user`, and `password` (which it will always do),
as well as the `database` (with a default value of `PROJECT_DB` filled in) and 
`schema` (with no default value filled in). When the connection to Snowflake is made
the `account` will be hard-coded as `XXX` and the `warehouse` will be hard-coded as
`PROJECT_WH`.

### Snowpark Python
To connect to Snowflake using the Snowpark Python with a login form,
you would use `st_snow.login.session()`:

```
import st_snow

## Things above here will be run before (and after) you log in

conn = st_snow.login.session()

## Nothing below here will be run until you log in
```

At that point you can use the session just like you would otherwise. If
the connection gets severed, the login form will be presented again.

The login function takes 2 arguments:
* `form_options`: a dictionary. The keys of this dictionary will become input
fields for the user to fill in (e.g, `user`). The values of the keys will be
the default value (leave `""` for no default value).
* `options`: a dictionary. These options will be passed as-is to the connection.
For example, if you wanted to set a `timezone` and not allow a user to edit that,
you could put it in `options`. If you wanted them to edit it, you would put it
in `form_options`.

For example:

```
import st_snow

conn = st_snow.login.session({'database': 'PROJECT_DB', 'schema':''}, {'account': 'XXX', 'warehouse': 'PROJECT_WH'})
```

This would create a form to collect the `account`, `user`, and `password` (which it will always do),
as well as the `database` (with a default value of `PROJECT_DB` filled in) and 
`schema` (with no default value filled in). When the connection to Snowflake is made
the `account` will be hard-coded as `XXX` and the `warehouse` will be hard-coded as
`PROJECT_WH`.


## *Experimental Feature (Automatic Caching of Result Sets)*
It is a best practice in Streamlit to cache data that is retrieved
from a data store so as to have a more responsive application. To support
this, this package has a sub-package `st_snow.cached` to support automatically
caching result sets without having to declare `@st.cache` or `@st.experimental_memo`.

This package provides for a new type of `SnowflakeCursor` that will automatically
cache itself based on the SQL that was executed (if it is the same SQL (and it is
in the cache) then the cached result will be returned instead of retrieving it from
Snowflake). The SQL must be identical - including parameters, etc.

All results are cached in the user's `st.session_state`. That is, there is no sharing of 
results across sessions (in global state). Even in the singleton pattern the 
results are stored in the user session. So, the Streamlit users will share connection
to the databsae, but will have their one result set cache.

To simplify the experience, this package provides a new class that derives from
`snowflake.connector.SnowflakeConnection` and `snowflake.snowpark.Session` and 
fully encapsulates the caching from the developer. The access methods are
the same as above, but instead of `st_snow` you use `st_snow.cached`.

### Singleton Connections
The API is the same as above:

```
import json
import st_snow

def connect_to_snowflake(fname):
    snowcreds = json.load(open(fname, "r"))
    return st_snow.cached.singleton.connection(**snowcreds)

conn = connect_to_snowflake("/path/to/json/credentials.json")
```

And

```
import json
import st_snow

def connect_to_snowflake(fname):
    snowcreds = json.load(open(fname, "r"))
    return st_snow.cached.singleton.session(snowcreds)

session = connect_to_snowflake("/path/to/json/credentials.json")
```

### Login Form
The API is the same as above:

```
import st_snow

## Things above here will be run before (and after) you log in

conn = st_snow.cached.login.connection()

## Nothing below here will be run until you log in
```

And

```
import st_snow

## Things above here will be run before (and after) you log in

conn = st_snow.cached.login.session()

## Nothing below here will be run until you log in
```
