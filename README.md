# Firebolt-CLI
Firebolt cli is a tool for connecting to firebolt, managing firebolt resources, and execute queries from the command line.


## Quick start

---
### Prerequisites
Before installation, `python` and `pip` should be installed beforehand. For this go to [Python official page](https://www.python.org/downloads/). 

Also make sure, you have a firebolt username, password, database and an engine. For more information go to [Firebolt](https://firebolt.io).

### Installation
Once you have all prerequisites in place, you can install the firebolt-cli via pip:
```
$ pip install firebolt-cli
```

In order to verify the installation run:
```
$ firebolt --version

firebolt, version 0.0.1
```

### Running
The next step is to configure the firebolt cli:
```
$ firebolt configure

Username [None]: your_username
Password [None]: ********
Account name [None]: your_firebolt_account_name
Database name [None]: your_database
Engine name or url [None]: your_engine_name_or_url
Created new config file
```

In order to run your first query, the engine has to be in a running state, first check the status of engine using following command:
```
$ firebolt engine status --name your_engine_name

Engine your_engine_name current status is: ENGINE_STATUS_SUMMARY_STOPPED
```

If the engine is stopped, you have to start the engine by running:
```
$ firebolt engine start --name your_engine_name

Engine your_engine_name is successfully started
```

Now you are ready to run your first query, this could be done by opening the interactive query 
```
$ firebolt query

Connection succeded
firebolt> SELECT * FROM your_table LIMIT 5;
+--------------+-------------+-------------+--------------+
|   l_orderkey |   l_partkey |   l_suppkey | l_shipdate   |
+==============+=============+=============+==============+
|      5300614 |       66754 |        4273 | 1993-02-06   |
+--------------+-------------+-------------+--------------+
|      5300614 |      131772 |        6799 | 1993-02-21   |
+--------------+-------------+-------------+--------------+
|      5300615 |      106001 |        8512 | 1997-12-10   |
+--------------+-------------+-------------+--------------+
|      5300615 |      157833 |        7834 | 1997-12-01   |
+--------------+-------------+-------------+--------------+
|      5300640 |       36106 |        8610 | 1994-09-10   |
+--------------+-------------+-------------+--------------+
firebolt>
```


## Usage

---
With firebolt cli you can manage the databases and engines, as well as run SQL quires.
```
Usage: firebolt [OPTIONS] COMMAND [ARGS]...

  Firebolt command line utility.

Options:
  -V, --version  Show the version and exit.
  --help         Show this message and exit.

Commands:
  configure  Store firebolt configuration parameters in config file
  database   Manage the databases
  engine     Manage the engines
  query      Execute sql queries
```


### Configure
There is three ways to configure firebolt-cli.
1. By running `firebolt configure`

It is possible to either run `firebolt configure` and enter all parameters from STDIN.
Or you can set particular parameters by running configure with additional command line arguments:
```
firebolt configure --username your_user_name --password-file /path/to/pswd
```

2. By passing additional command-line arguments to each command.

```
firebolt query --username your_user_name --password-file /path/to/pswd
```

3. Using environment variable
```
$ export FIREBOLT_USERNAME=your_username
$ export FIREBOLT_ACCOUNT_NAME=your_account_name
$ export FIREBOLT_API_ENDPOINT=api_endpoint
$ export FIREBOLT_ENGINE_NAME_URL=your_engine_name_or_url
$ firebolt query
```
Note, tha it is not possible to set password via command-line interface.

### Engine

### Database



## Docker 

---

## Contributing

---
