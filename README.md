# Apache Logs to MySql

This script helps you to convert your apache logs into mysql, that can be inserted into mysql for further monitoring and quering.

#Usage

Basic
```sh
index.py apacheAccessLogPath tableName
```

Advanced (Will create a table of said name in MySql and update the data in it)
```sh
index.py apacheAccessLogPath tableName host database user pass
```


