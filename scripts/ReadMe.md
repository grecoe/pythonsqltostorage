# Scripts

This directory is used to hold the SQL scripts, but can be wherever you want. Look at config.json for each of the sqlscript objects script paths. 

These scripts are used against a database that contains a single table Employee:

```
CREATE TABLE Employee (
    PersonID int,
    LastName varchar(255),
    FirstName varchar(255),
    Address varchar(255),
    City varchar(255)
);
```