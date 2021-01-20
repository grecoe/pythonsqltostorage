# Copyright (c) Microsoft Corporation.

import os
from utils import StorageUtils # pylint: disable=E0401,E0611


# Storage account connection string collected from portal.
STORAGE_CONNECTION_STRING = "YOUR_CONNECTION_STRING"

# Create the storage util object to perform work on account
store_util = StorageUtils(STORAGE_CONNECTION_STRING)

"""

List containers and optionally blobs for stuff in a storage accoount.

"""
print("Account Containers")
account_containers = store_util.list_containers()
for container in account_containers:
    print("\t", container)

    # Optionally list out blobs within a container
    #blobs = store_util.list_blobs(container)
    #for blob in blobs:
    #    print("  ", blob)



"""

Create a container (if not exists) and upload a blob (overwrite if does exist)

"""
UPLOAD_CONTAINER = "weblogs"
UPLOAD_SLUG = './uploadslugs/3d_farstack.sgy'
BLOB_TARGET = "doc/dummy.sgy"




# Create container (if it doesn't exist) and upload files from the slug folder
new_container = store_util.create_container(UPLOAD_CONTAINER) 

# Using new container upload one of the slug files
upload_sas_token = None
if new_container:
    if os.path.exists(UPLOAD_SLUG):
        """
        Upload produces a full URI with SAS token to blob, this can be used by other
        downstream services to be able to pull the file with a standard requests.get. 
        """
        upload_sas_token = store_util.upload_blob(UPLOAD_CONTAINER, BLOB_TARGET, UPLOAD_SLUG )
        print("Word Document SAS Tokenized URI\n\t{}".format(upload_sas_token))

"""

Download a blob to the local machine, use the blob from above. In this case, we will not
use the SAS URI, we will find a blob and then pull it. 

"""
success_download = store_util.download_blob(UPLOAD_CONTAINER, BLOB_TARGET, "./downloads")
print("Blob retrieved - {}".format(success_download))