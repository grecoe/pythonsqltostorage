# Copyright (c) Microsoft Corporation.

import json
import os
from datetime import datetime, timedelta
from utils.parse import ParseUtilities
from utils.tracelog import FunctionTrace
from azure.storage.blob import generate_blob_sas,generate_container_sas, BlobServiceClient,BlobSasPermissions # pylint: disable=E0401,E0611

# https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python#from-container-url-container-url--credential-none----kwargs-
# ContainerClient

class StorageUtils:
    """
        Helper class for Azure Storage Functionlity.
    """

    def __init__(self, storage_connection_string, trace_logging=True):
        """
        Constructor for StorageUtils class

        Params:
        storage_connnection_string : 
            Full connection string to Azure Storage account
        trace_logging :
            Boolean if true, wraps the storage calls with a decorator. This will generate a 
            log file called storage.log that will show all calls, arguments, returns and 
            timing for each call. 
        """
        self.trace_logging = trace_logging
        self.connection_string = storage_connection_string
        self.account_name, self.account_key = ParseUtilities.parse_connection_string(
            self.connection_string
        )

        if self.trace_logging:
            self.list_containers = FunctionTrace(self.list_containers)
            self.list_blobs = FunctionTrace(self.list_blobs)
            self.create_container = FunctionTrace(self.create_container)
            self.upload_blob = FunctionTrace(self.upload_blob)
            self.download_blob = FunctionTrace(self.download_blob)
            self.generate_sas_token_blob = FunctionTrace(self.generate_sas_token_blob)
            self.generate_uri = FunctionTrace(self.generate_uri)

    def list_containers(self, get_public_access=False):  # pylint: disable=method-hidden
        """
        Find all containers within a storage acount

        Params:
        get_public_access - 
            Optional to turn on, will return public access level along with name


        Returns:
        get_public_access == False
            List of container names (str) found
        get_public_access == True
            List of lists with each list [name, access level]
        """
        blob_srv_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )

        container_names = []
        container_client = blob_srv_client.list_containers()

        for ctr in container_client:
            if get_public_access:
                container_names.append([ctr.name,ctr.public_access])
            else:                
                container_names.append(ctr.name)

        return container_names

    def list_blobs(self, container_name): # pylint: disable=method-hidden
        """
        Find all blobs within a storage container

        Params:
        container_name - Storage container to list, if not present container is created

        Returns:
        List of blob names (str) found
        """
        container_client = self.create_container(container_name)

        blob_names = []

        for blob in container_client.list_blobs():
            blob_names.append(blob.name)

        return blob_names            

    def create_container(self, container_name): # pylint: disable=method-hidden
        """
        Retrieve a container client for the current storage account, if the container
        does not exist, it is created.

        Params:
        container_name : Name of container to obtain the container client

        Returns:
        An instance of the container client.
        """
        blob_srv_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        container_names = [x["name"] for x in blob_srv_client.list_containers()]
        if container_name not in container_names:
            blob_srv_client.create_container(container_name)

        container_client = blob_srv_client.get_container_client(container_name)
        return container_client

    def upload_blob(self, container_name, obj_path, obj, retry_count=3): # pylint: disable=method-hidden
        """
        Upload a blob to an Azure Storage account.

        Params:
        container_name : Name of container to receive the blob
        obj_path : Full path to the object in the container, i.e. /path/path/file.ext
        obj : Data to store in blob - this is a local file on disk.

        Returns:
        On success, the full URI to the new blob complete with SAS Token
        """
        return_value = None
        attempts = 0
        retry_count = int(retry_count)

        container_client = self.create_container(container_name)

        while return_value is None and attempts < retry_count:
            # Container name, connection string, token
            if container_client and obj_path and obj:

                with open(obj, "rb") as stream:
                    try:
                        blob_client = container_client.get_blob_client(obj_path)

                        # If the blob by this name already exists, delete it
                        blobs = [x["name"] for x in container_client.list_blobs()]
                        if obj_path in blobs:
                            container_client.delete_blob(obj_path)

                        # Upload the streamed object
                        blob_client.upload_blob(stream, blob_type="BlockBlob")

                        # Get the SAS token of the uploaded blob
                        sas_token = self.generate_sas_token_blob(container_name, obj_path)
                        
                        # Generate the full URI with SAS token for return
                        return_value = self._generate_blob_uri(
                            self.account_name, container_name, obj_path, sas_token
                        )

                        print("Blob uploaded : {}".format(obj_path))
                    except Exception as ex:  # pylint: disable=W0703
                        print("Blob upload exception: {}".format(str(ex)))

            attempts += 1

        return return_value

    def download_blob(self, container_name, blob_path, local_directory): # pylint: disable=method-hidden
        """
        Download a blob file from a storage account

        Parameters:
        container_name = Name of the container to pull the blob from
        blob_path = Full blob path (i.e. blob.name) -> path/path/file.ext
        local_directory = Path to a directory on this system

        Returns:
        True if downloaded, False otherwise


        NOTES:
        If the directory targeted locally doesn't exist, it is created.
        
        The file generated is the file name of the blob in the local_directory without 
        additional path information used in blob store.

        If the target local file exists it is deleted first. 
        """
        # Return flag upon success
        blob_downloaded = False

        local_destination = None

        # Does destination folder exist?
        if not os.path.exists(local_directory):
            os.makedirs(local_directory)

        # Is the blob path a combination of path/file? 
        if '/' in blob_path:
            local_destination = os.path.join(local_directory, blob_path.split('/')[-1])
        elif '\\' in blob_path:
            local_destination = os.path.join(local_directory, blob_path.split('\\')[-1])
        else:
            local_destination = os.path.join(local_directory, blob_path)

        # If the file already exists, delete it
        if os.path.exists(local_destination):
            os.remove(local_destination)

        # Get the container
        blob_container = self.create_container(container_name)

        # If the blob by this name already exists, delete it
        blobs = [x["name"] for x in blob_container.list_blobs()]
        if blob_path in blobs:
            # Storage Stream Downloader
            # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.storagestreamdownloader?view=azure-python#readinto-stream-
            ss_downloader = blob_container.download_blob(blob_path)

            with open(local_destination, "wb") as blob_target_location:
                ss_downloader.readinto(blob_target_location)

            if os.path.exists(local_destination):
                blob_downloaded = True

        return blob_downloaded

    def generate_sas_token_blob(self, container_name, blob_name, valid_for=7): # pylint: disable=method-hidden
        """
        Generate a SAS token for a given blob.

        Params:
        container_name : Container where the blob lives.
        blob_name : The name (full path) of the blob in the container.
        valid_for : Number of minutes, from now, that the token is valid

        Returns:
        String SAS token for a blob
        """
        sas_token = generate_blob_sas(
            account_name=self.account_name,
            account_key=self.account_key,
            container_name=container_name,
            blob_name=blob_name,
            permission=BlobSasPermissions.from_string("r"),
            start=datetime.utcnow(),
            expiry=datetime.utcnow() + timedelta(days=valid_for),
        )

        sas_token = "?" + sas_token

        return sas_token

    def generate_sas_token_container(self, container_name, valid_for=7):
        
        sas_token = generate_container_sas(
            account_name=self.account_name,
            account_key=self.account_key,
            container_name=container_name,
            permission=BlobSasPermissions.from_string("racwd"),
            start=datetime.utcnow(),
            expiry=datetime.utcnow() + timedelta(days=valid_for),
        )

        sas_token = "?" + sas_token

        return sas_token        

    def generate_uri(self, container, blob_name, sas_token): # pylint: disable=method-hidden
        """
        Generate a URI with SAS token for Azure Blob

        Parameters:
        container - Container name
        blob_name - Remainder of blob path
        sas_token - SAS Token retrieved for blob

        REturns:
        String
        """
        return self._generate_blob_uri(
            self.account_name, container, blob_name, sas_token
        )

    def _generate_blob_uri(
        self, account, container, blob_path, sas_token
    ):  # pylint: disable=R0201
        """
        Generate blob URI with SAS token

        Params:
        account : Storage account name
        container : Storage Container Name
        blob_path : Remaining path to blob
        sas_token : SAS Token

        Returns:
        URI of blob
        """

        # if ' ' in blob_path:
        #    # Service issue with spaces in URI, path is the only place this can occur.
        #    blob_path = blob_path.replace(' ', '%20')

        uri = "https://{account}.blob.core.windows.net/{container}/{blob_name}{sas_token}".format(
            account=account,
            container=container,
            blob_name=blob_path,
            sas_token=sas_token,
        )

        return uri

 