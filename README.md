# Python SQL to Storage Utility

This package contains conde that can be used to automate

1. Running SQL scripts stored in .sql files
2. Creating CSV and/or PARQUET output files
3. Uploading output files to Azure Storage. 
## Setup
To use this code you must create a conda environment that will import the Azure Blob libraries as well as the pyodbc library.

```
conda env create -f environment.yml
```

## Configuration
In this directory is a file called config.json. This is what drives the flow of the application and there are some specific data to point out:

|Parent|Child|Details|
|------|-----|-------|
|context|temp_directory|Temporary directory to store SQL query output prior to uploading to Azure Storage. This will be created and deleted on each run of the script.|
|storage|connection|Full connection string to Azure Storage account in which the query results will be uploaded to.|
|storage|container|The storage container in the storage account in which to upload the query results.|
|storage|format|Output results for the data (and to be uploaded to Azure). The options are:<br><li>csv</li><li>parquet</li><li>both</li> <br><br>'csv' will produce a csv file with the query content for upload to storage.<br><br>'parquet' will produce a .parquet file with the query content for upload to storage.<br><br>'both' will produces both a csv and parquet file for upload to storage|
|sql|all|These are the connection data required to connect to your SQL server to execute daily queries on.|
|sqlscripts|N/A|A list of SQL scripts that are to be run during the application execution. The following table entries identify the data required for each object.|
||database|The database on which to execute the script file.|
||script|Path to the actual script file to execute.|
||blob_path|Formatted with "%Y/%m/%d/" and will be formatted with the current date information.<br><br>No file name should be added here. The file name is taken from teh 'script' field by stripping the file name and replacing .sql with .XXX for whatever output we are tracking (csv or parquet). |

<sub><b>NOTE</b> All script output is currently stored in CSV format. This, however, can be modified to store into parquet files by including the Pandas library which has not yet been done.</sub>

## Program
The following steps describe what is going on in the application:

- Load the config.json file and convert it into an object for further usage. 
- Create an instance of StorageUtil for uploading results. 
- Generage an SQLUtil instance and
    - For each script in sqlscripts
        - Execute the SQL file 
        - Dump results as CSV to the temp directory
        - Record output file and script block executed
- For each executed script, upload the output to the blob storage account.
- Delete the temp directory.
