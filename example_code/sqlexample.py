# Copyright (c) Microsoft Corporation.

"""
Example using pyodbc, create an environment and execute 

pip install pyodbc

You will need an 
- SQL Server
- Table
- Username
- Password

Then you can either execute queries you generate here or queries stored in a file. 
Results can be saved to a CSV file or simply printed out. 

Fo the example below, it assumes a database with the following table:

CREATE TABLE Employee (
    PersonID int,
    LastName varchar(255),
    FirstName varchar(255),
    Address varchar(255),
    City varchar(255)
);
"""
from utils import SqlConn # pylint: disable=E0401,E0611

server = 'YOURSERVER.database.windows.net'
database = 'DATABASENAME'
username = 'USER'
password = 'PASSWORD'   
driver= '{ODBC Driver 17 for SQL Server}'

table_name = "Employee"

with SqlConn(server,username, password, driver) as sql:

    query_result = sql.execute_sql(database, "./scripts/all.sql")
    for r in query_result:
        # Note that results return a python object that have made column
        # values first class properties of the object.
        print("You found:", r.FirstName)

    # Save your results as a CSV, if you want parquet, set query_format=QueryFormat.PARQUET
    sql.execute_sql(database, "./scripts/all.sql", output_file='query_results.txt')


