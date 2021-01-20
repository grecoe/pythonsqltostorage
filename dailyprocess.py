# Copyright (c) Microsoft Corporation.

import os
import shutil
import json
#import pandas
from datetime import datetime
from utils import Config, SqlConn, QueryFormat, StorageUtils


# Load configuration and create storage from it.
app_configuration = Config("config.json")
storage_utility = StorageUtils(app_configuration.storage.connection, trace_logging=False)


# Make sure temp directory is present for script outputs
if not os.path.exists(app_configuration.context.temp_directory):
    os.makedirs(app_configuration.context.temp_directory)

# With SQL Connection, get all execution details. 
execution_details = {}

with SqlConn(
    app_configuration.sql.server,
    app_configuration.sql.user, 
    app_configuration.sql.password, 
    app_configuration.sql.driver,
    trace_logging=False) as sql:

    # Ensure the formatting option is acceptable
    format_options = set(item.value for item in QueryFormat)
    selected_format = app_configuration.storage.format.lower().strip()

    if selected_format not in format_options:
        raise Exception("Selected format {} not in formatting options {}".format(selected_format, format_options))

    selected_format = QueryFormat(selected_format)
    if selected_format == QueryFormat.RAW:
        raise Exception("Raw format {} not accepted, choose a different one from- {}".format(selected_format, format_options))

    print("Selected format {} has been accepted.".format(selected_format))

    # If here, we are outputing csv, parquet, or both. Now loop through the scripts and execute them.
    for script in app_configuration.sqlscripts:

        # The base output file name is the SQL script file name without the extension.
        base_output_filename = script["script"]
        base_script_name = os.path.split(script["script"])[1]
        base_output_filename = base_script_name.split('.')[0]

        # Build path for output file which is path and file name without extension
        base_output_filename = os.path.join(
                    app_configuration.context.temp_directory, 
                    base_output_filename 
                )

        # Execute the SQL script now
        print("Executing Script:", base_script_name)
        return_files = None
        try:

            return_files = sql.execute_sql(
                script["database"], 
                script["script"],
                selected_format,
                base_output_filename
            )            

        except Exception as ex:
            print("Execution failed : ", str(ex))
            return_files = None

        if return_files:
            # If we have a return (not in exception case) save it so we can upload the results
            execution_details[base_script_name] = { "script" : script, "content" : return_files}



# Scripts have been executed and script details/output files are now in the 
# execution_details object. 
#
# Go through each output and upload to blob storage. For every path (should be one)
# upload a .complete file next. 
blob_paths_used = []
execution_summary = { "date" : str(datetime.utcnow())}

for script_name in execution_details:
    blob_name_path = execution_details[script_name]["script"]["blob_name"]
    blob_name_path = datetime.utcnow().strftime(blob_name_path)

    if blob_name_path[-1] != '/':
        blob_name_path += '/'

    execution_summary['blob_path'] = blob_name_path
    if blob_name_path not in blob_paths_used:
        blob_paths_used.append(blob_name_path)

    tokenized_uris = []
    for local in execution_details[script_name]["content"]:
        blob = "{}{}".format(blob_name_path, os.path.split(local)[1])

        upload_sas_token = storage_utility.upload_blob(
            app_configuration.storage.container, 
            blob, 
            local )

        tokenized_uris.append(upload_sas_token)

    execution_summary[script_name] = tokenized_uris


# Now put a .complete in each folder path where blobs were uploaded (just above)

# CHANGE THIS TO PUT IT WHERE IT REALLY SHOULD BE
COMPLETE_TO_ROOT = True
if len(blob_paths_used):

    execution_summary['complete'] = []

    complete_path = os.path.join(
                    app_configuration.context.temp_directory, 
                    ".complete" 
                )

    # Generate .complete file
    with open(complete_path, 'w') as complete_file:
        complete_file.write(",".join(blob_paths_used))

    # Now upload a .complete into any blob path that was utilized.
    for comp_content in blob_paths_used:
        if COMPLETE_TO_ROOT:
            comp_blob = ".complete" 
        else:
            comp_blob = "{}.complete".format(comp_content)

        execution_summary['complete'].append(comp_blob)

        print("Upload .complete file", comp_blob)
        storage_utility.upload_blob(
            app_configuration.storage.container, 
            comp_blob, 
            complete_path )

# Dump out every thing we know for the caller. 
print("Execution Summary Summary")
print(json.dumps(execution_summary, indent=4))

# Done, clean up temp directory
print("Clean up temp directory")
shutil.rmtree(app_configuration.context.temp_directory)