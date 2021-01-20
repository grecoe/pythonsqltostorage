# Copyright (c) Microsoft Corporation. All rights reserved.

import os
import logging


class ParseUtilities:
    """
        Static class used for generic, but frequent, parsing routines.
    """

    @staticmethod
    def parse_filename(file_name: str):
        """
        Given a file extension, determine if the file type is currently
        allowed in processing.

        Params:
        file_name : Path to a file generically in the form of
        container/filename

        Returns:
        Tuple of container_name, file_name, file_extension
        """
        parts = os.path.split(file_name)

        container = None
        return_name = None
        extension = None
        if len(parts) == 2 and len(parts[1]) > 0:
            container = parts[0]
            return_name = parts[1]
            if "." in return_name:
                extension = return_name.split(".")[-1]

        return container, return_name, extension

    @staticmethod
    def parse_connection_string(storage_connection : str):
        """
        Parse a storage connection string to the commmonly used
        parts.

        Params:
        storage_connection : Full connection string to storage

        Returns:
        Tuple of storage_account_name, storage_account_key

        If invalid one or both of the return values will be None
        """
        ACCOUNT_KEY = "AccountKey="  # pylint: disable=C0103
        ACCOUNT_NAME = "AccountName="  # pylint: disable=C0103
        
        account_key = None

        account_name = ParseUtilities._get_conn_value(storage_connection, ACCOUNT_NAME)
        if account_name:
            account_key = ParseUtilities._get_conn_value(storage_connection, ACCOUNT_KEY)

        return account_name, account_key

    @staticmethod
    def parse_blob_url(blob_uri):
        """
        Parse a full blob URI to the commmonly used parts.

        Params:
        blob_uri : Full blob URI with or without SAS token

        Returns:
        Dictionary with the following properties:
        account : Storage account name
        container : Storage container
        blob_path : Remainder of blob path with file name
        file_name : Blob file name
        file_extension : Blob file extension
        sas : SAS token if provided in original URI

        IF the URI does not represent an actual blob URI, then an empty array is returned.
        """
        return_value = {}
        
        account_name = None
        container = None
        sas = None
        blob_path = None

        hyperlink_preamble = '://'
        if blob_uri:
            if hyperlink_preamble in blob_uri:
                idx = blob_uri.index(hyperlink_preamble)
                idx2 = -1

                if "?" in blob_uri:
                    idx2 = blob_uri.index("?")

                # Break path from SAS token.
                idx += len(hyperlink_preamble)
                sas = None
                if idx2 != -1:
                    path = blob_uri[idx:idx2]
                    sas = blob_uri[idx2:]
                else:
                    path = blob_uri[idx:]

                # Now break uri and path, if there are no separators then we 
                # don't have a blob_uri
                if '/' in path:
                    idx = path.index("/")
                    uri = path[:idx]
                    blob_path = path[idx + 1 :]

                    # URI must have a dot (for account) and path must have a /
                    # that determines container. 
                    if '.' in uri and '/' in blob_path:
                        # get account from URI
                        idx = uri.index(".")
                        account_name = uri[:idx]

                        # Get the container and path
                        idx = blob_path.index("/")
                        container = blob_path[:idx]
                        blob_path = blob_path[idx + 1 :]

                        # Get the blob path
                        file_name = None
                        if "/" not in blob_path:
                            file_name = blob_path
                        else:
                            path_parts = os.path.split(blob_path)
                            file_name = path_parts[-1]

                        extension = None
                        if "." in file_name:
                            idx = file_name.split(".")
                            extension = idx[-1]

                        return_value["account"] = account_name
                        return_value["container"] = container
                        return_value["blob_path"] = blob_path
                        return_value["file_name"] = file_name
                        return_value["file_extension"] = extension
                        return_value["sas"] = sas

        return return_value

    @staticmethod
    def _get_conn_value(connection_string, value):
        """
        Retrieve a part of a connection string

        Params:
        connection_string : Connection string
        value : Name of property to retrieve, i.e. Endpoint=,
        that is, complete with equal sign.

        Returns:
        The value between 'value' and the trailing semi-colon. If
        it is the last item, an exception is thrown, and caught and
        the remainder of the connection string is returned.
        """
        return_value = None
        if value in connection_string:
            idx = connection_string.index(value) + len(value)

            try:
                idx2 = connection_string.index(";", idx)
                return_value = connection_string[idx:idx2]
            except Exception as ex:  # pylint: disable=W0703
                logging.info("End of connection string, take last {}".format(str(ex)))  # pylint: disable=W1202
                return_value = connection_string[idx:]

        return return_value