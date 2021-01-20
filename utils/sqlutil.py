# Copyright (c) Microsoft Corporation.

from enum import Enum
import pyodbc # pylint: disable=E0401,E0611
import pandas as pd # pylint: disable=E0401,E0611
from utils.tracelog import FunctionTrace

class QueryFormat(Enum):
    """
    Supported formats for a query.

    CSV -> Saves as CSV or returns [SqlData]
    PARQUET -> Saves as PARQUET or returns DataFrame
    """
    RAW= 'raw'
    CSV = 'csv'
    PARQUET = 'parquet'
    BOTH = 'both'

class SqlData:
    """
    Represents a row of data from a query 
    """
    def __init__(self, columns, data):
        if len(columns) != len(data):
            raise Exception("Columns must equal data {} != {}".format(len(columns), len(data)))

        for idx in range(len(columns)):
            setattr(self, columns[idx], data[idx])


class SqlConn:
    """
    SQL Connection class to hide details of executing a query and saving data
    to a file. Support for CSV and PARQUET files currently. 
    """
    def __init__(self, server, user, credential, driver, trace_logging=True):
        self.connections = {}
        self.server = server
        self.user = user
        self.credential = credential
        self.driver = driver

        self.trace_logging = trace_logging
        if self.trace_logging:
            self.execute_sql = FunctionTrace(self.execute_sql)
            self._yeild_execute = FunctionTrace(self._yeild_execute)

    def execute_sql(self, database, sql_file, query_format=QueryFormat.CSV, output_file_format=None): # pylint: disable=method-hidden
        """
        Execute a SQL query stored in a file and either return the results OR write them to a file
        on disk. 

        Parameters:
        database:
            String, database name to execute query on
        sql_file:
            File containing the SQL statement to execute on the database.
        query_format:
            Enum value from QueryFormat
        output_file_format:
            Optional local disk file pattern to store results to. If CSV/PARQUET/BOTH are selected
            this output file format will be used as
            query_format == QueryFormat.CSV
                output_file_format + '.csv' 
            query_format == QueryFormat.PARQUET
                output_file_format + '.parquet' 
            query_format == QueryFormat.BOTH
                output_file_format + '.csv' and output_file_format + '.parquet'

        Returns:
            query_format == QueryFormat.RAW
                List of SqlData objects regardless of output_file_format, never saved to disk
            query_format == QueryFormat.CSV
                If output_file_format is specified, a CSV file is written, otherwise returns 
                a pandas DataFrame raw
            query_format == QueryFormat.PARQUET
                If output_file_format is specified, a PARQUET file is written, otherwise returns 
                a pandas DataFrame raw
            query_format == QueryFormat.BOTH
                If output_file_format is specified, a PARQUET and CSV file is written, otherwise returns 
                a pandas DataFrame raw
        """
        return_value = None
        file_based_query = None

        # Load the actual query from the disk file
        with open(sql_file,'r') as sql_data:
            file_based_query = sql_data.readlines()
            file_based_query = "\n".join(file_based_query)

        if query_format == QueryFormat.RAW:
            return_value = []
            for cols, row in self._yeild_execute(database, file_based_query):
                return_value.append(SqlData(cols, row))
        else:
            output_files = []
            connection = self._connect(database)
            return_value = pd.read_sql(file_based_query, connection)
            if output_file_format:
                if query_format == QueryFormat.PARQUET or query_format == QueryFormat.BOTH:
                    output_file = "{}.{}".format(output_file_format,QueryFormat.PARQUET.value )
                    return_value.to_parquet(output_file, compression=None)
                    output_files.append(output_file)
                if query_format == QueryFormat.CSV or query_format == QueryFormat.BOTH:
                    output_file = "{}.{}".format(output_file_format,QueryFormat.CSV.value )
                    return_value.to_csv(output_file, compression=None, index=False)
                    output_files.append(output_file)

                return_value = output_files

        """
        if query_format == QueryFormat.CSV:
            output_stream = None
            try:
                columns_output_to_file = False

                # If we have an output file identified, return is file name
                # otherwise return is list of SqlData objects
                if output_file_format:
                    output_stream = open(output_file_format, "w")
                    return_value = output_file
                else:
                    return_value = []

                # Call returns records one at a time from a query so we can stream it
                # to a file with less overhead OR build up the return list.
                for cols, row in self._yeild_execute(database, file_based_query):
                    data_instance = SqlData(cols, row)
                    if output_stream:
                        # If not done, dump columns
                        if not columns_output_to_file:
                            columns_output_to_file = True
                            output_stream.write("{}\n".format(",".join(cols)))

                        # Now dump out the content
                        data = []
                        for col in cols:
                            data.append(str(getattr(data_instance,col,'')))

                        output_stream.write("{}\n".format(",".join(data)))
                    else:
                        return_value.append(data_instance)

            finally:
                if output_stream:
                    output_stream.close()
        else:
            connection = self._connect(database)
            return_value = pd.read_sql(file_based_query, connection)
            if output_file:
                return_value.to_parquet(output_file, compression=None)
                return_value = output_file
        """

        return return_value         

    def _yeild_execute(self, database, query): # pylint: disable=method-hidden
        """
        Execute an SQL query, but return the rows one at a time to allow for streaming
        to a file. 

        Parameters:
        database:
            Name of database to execute query on
        query:
            Actual SQL query 

        Returns:
            yeilds on each individual row found
        """
        connection = self._connect(database)
        with connection.cursor() as cursor:
            cursor.execute(query)
            
            if cursor.description:
                columns = []
    
                for desc in cursor.description:
                    columns.append(desc[0])
    
                row = cursor.fetchone()
                while row:
                    if columns:
                        yield columns, row
                    row = cursor.fetchone()        
            else:
                # If no description we can find out if there are row counts 
                # which will reflect insert/delete rows affected, etc.
                yield ["rows"], [cursor.rowcount]

    def __enter__(self):
        """
        Support for using in a 'with' clause
        """
        return self

    def __exit__(self, type, value, traceback):
        """
        Support for using in a 'with' clause
        """
        self._disconnect()

    def _disconnect(self):
        """
        Close all open connections.
        """
        for conn in self.connections:
            self.connections[conn].close()

    def _connect(self, database):
        """
        Create a database connection of one does not already exist.

        Parameters:
        database:
            Name of the database for the connection

        Returns:
            Connection object
        """
        if database not in self.connections:
            if not self.server:
                raise Exception("Server must be identified")                                
            if not self.user:
                raise Exception("User must be identified")                                
            if not self.credential:
                raise Exception("User Credential must be identified")                                
            if not self.driver:
                raise Exception("ODBC Driver must be identified")   

            # 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
            conn_str = 'DRIVER={};SERVER={};PORT=1433;DATABASE={};UID={};PWD={}'.format(
                self.driver,
                self.server,
                database,
                self.user,
                self.credential
            )

            self.connections[database] = pyodbc.connect(conn_str)                                                                  

        return self.connections[database]