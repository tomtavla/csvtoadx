import os
import sys
import logging
import time
import tarfile
#import warnings
import re
#import pandas
#from functions import GetSubfolders
#from ast import literal_eval
import argparse
import pandas as pd
import glob
import shutil

from azure.kusto.data import DataFormat
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, ReportLevel, ReportMethod
from azure.kusto.data.data_format import DataFormat

# Import the required classes and methods from the azure.kusto.data Python package
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

from azure.kusto.data.helpers import dataframe_from_result_table

logging.basicConfig(level=logging.DEBUG)
#from azure.kusto.ingest.status import KustoIngestStatusQueued
from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.kusto import KustoManagementClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.kusto.models import ReadWriteDatabase
from datetime import timedelta

#ARGS

description = """\

Description
    This script does the following:
    1. Untars the PM stats tarballname.tar.gz to a subdirectory with the same named but stripped the .tar.gz →<tarballname>.
    2. Please be aware that only tarballname.tar.gz format is working for now.
    3. Merges the PM stats raw files in each group resulting in one file per group.
    4. Creates the ADX database where the data will be loaded to.
    5. Creates all tables based on the list of groups extracted from the raw data.
    6. Creates the schema based on the merged files in each group/table.
    7. Loads the data into ADX.
    8. Adds access permissions to the security group (SG) “AFO_AOI<afo-aoi@microsoft.com>” My Security Group Memberships (microsoft.com).
    9. Cleans up the untarred files but leaves the merged files in the pwd/tarballname/

Installation instructions:
    1. Install required packages
    2. The SP secret is required to be set as an environment variable: SPSECRET
    3. Obtain the secret from the ADX admin
    4. Set the environment variable: export SPSECRET="<the secret>"
    5. Run the script again with the -f and -d arguments to process the tarball

"""
parser = argparse.ArgumentParser(description)
#parser.print_help()
#print(description)

# Add your argument parsing code here
parser.add_argument("-f", "--filename", help = "Example: -f pmstats.tar.gz", required = True, default = "")
parser.add_argument("-d", "--adxdb", help = "Example: -d myadxdb", required = True, default = "")
parser.add_argument("-s", "--source", help = "Example: -s ems or ts", required = False, default = "ems")
parser.add_argument("-t", "--template", help = "Example: -t /mypath/DASHTEMP.json", required = False)
argument= parser.parse_args()

#get the item from the argument
if argument.filename:
    tarball_path = argument.filename  # tarball file with path
    file_name = argument.filename
    file_name = file_name.split("/")[-1].rstrip('.tar.gz') # tarball name without the .tar.gz and stripped path
if argument.adxdb:
    KUSTO_DATABASE = argument.adxdb
    KUSTO_DATABASE = KUSTO_DATABASE.strip()
if argument.source:
    CSVSOURCE = argument.source
    CSVSOURCE = CSVSOURCE.strip()
if argument.template:
    DASHTEMPLATE = argument.template
elif argument.source == "ts":
    DASHTEMPLATE = "/data/share/TSDASHTEMP.json"
else:
    DASHTEMPLATE = "/data/share/DASHTEMP.json"
#END ARGS

# Record the start time
start_time = time.time()
print("_________________________________________________________________")
print("Start time: " + str(start_time))
print("_________________________________________________________________")

pwd = os.getcwd()
print("Processing tarball: " + tarball_path)

#extract tarball if ems
def extract_tarball(tarball_path, destination_directory):
    with tarfile.open(tarball_path, 'r') as tar:
        for member in tar.getmembers():
            # Extracting the file with basename
            member.name = os.path.basename(member.name)
            tar.extract(member, destination_directory)

# Replace 'destination_directory' with the directory where you want to extract the files
if CSVSOURCE == "ems":
    destination_directory = os.path.join(pwd,file_name,"extracted_files")
elif CSVSOURCE == "ts":
    destination_directory = os.path.join(pwd,file_name,"varlog/psm.lnk")

# Extract the tarball
if CSVSOURCE == "ems":
    try:
        # Attempt to create the subdirectory
        os.makedirs(tarball_path.rstrip('.tar.gz'))
    except FileExistsError:
        # Directory already exists
        pass
    except Exception as e:
        # Handle other exceptions
        print(f"Error creating directory: {e}")
        sys.exit(0)
    extract_tarball(tarball_path, destination_directory)
    print("Tarball extracted successfully to: " + destination_directory)
elif CSVSOURCE == "ts":
    try:
        os.listdir(file_name.rstrip('.tar.gz'))
        if os.path.exists(file_name.rstrip('.tar.gz')):
            print(f"TechSupport directory {file_name.rstrip('.tar.gz')} found, skipping extraction")
    except FileNotFoundError as e:
        print(f"TechSupport directory {file_name.rstrip('.tar.gz')} not found, extracting now...")
        try:
            with tarfile.open(tarball_path, 'r') as tar:
                tar.extractall(pwd)
            print(f"Successfully extracted {file_name} to {os.path.join(pwd,file_name.rstrip('.tar.gz'))}")
        except Exception as e:
            print(f"Error extracting {tarball_path}: {e}")

# create a list of all unique filenames that result to all PM group names
# merge csv files
def merge_csv_files(dest_dir,work_dir):
    import os
    import re
    import glob
    import shutil
    import pandas as pd
    # Get the list of files in the current directory
    files = os.listdir(dest_dir)

    # Initialize an empty set to store unique filenames
    unique_filenames = set()

    # Regular expression pattern to match filenames with uppercase characters until the dash "-"
    pattern = r'^[A-Z_]+-.*\.csv$'

    # Iterate through each file
    for file in files:
        # Check if the file matches the pattern
        if re.match(pattern, file):
            # Extract the filename until the dash symbol "-"
            filename = file.split('-')[0]
            # Add the filename to the set of unique filenames
            unique_filenames.add(filename)

    # Return the set of unique filenames
    unique_filenames = sorted(list(unique_filenames))
    # Print the unique filenames
    print("Unique filenames matching the pattern:")
    if unique_filenames == []:
        print("No PM groups found in the extracted files.")
        sys.exit(0)
    for filename in unique_filenames: 
        print("Found PM group: " + str(filename))
 
    for group in unique_filenames:
        # Get a list of all CSV files matching the pattern
        files = glob.glob(f'{dest_dir}/{group}-*.csv')

        # Initialize an empty list to hold all dataframes
        df_list = []

        # Loop through each file and read it as a dataframe, skipping the first line
        for file in files:
            # Skip first line while reading the CSV file
            df = pd.read_csv(file)
            df = df.drop(df.columns[-1], axis=1)
            df = df.rename(columns={df.columns[i]: df.columns[i].replace("#", "").replace("-", "_").replace(":","_").replace(" ","_").replace(".","_") for i in range(len(df.columns))})
            df_list.append(df)

        # Concatenate all dataframes into a single dataframe
        merged_df = pd.concat(df_list, ignore_index=True)

        # Write the merged dataframe to a new CSV file in the subdirectory named from the file_name (tarball) given in ARGS
        merged_df.to_csv(os.path.join(work_dir, f'{group}.csv'), index=False)
        print(f"Merged CSV files successfully into {group}.csv")

    # Clean up files and directories
    # Remove the directory
    if CSVSOURCE == "ems":
        try:
            shutil.rmtree(dest_dir)
            print("Extract directory removed successfully.")
        except OSError as e:
            print(f"Error: {dest_dir} : {e.strerror}")

# Create a subdirectory to store the merged CSV files for ts and set the mergedcsvdir variable for ts or ems
if CSVSOURCE == "ts":
    os.makedirs(os.path.join(pwd,file_name,"varlog/psm.lnk/mergedcsvs"), exist_ok=True)
    mergedcsvdir = os.path.join(pwd,file_name,"varlog/psm.lnk/mergedcsvs")
elif CSVSOURCE == "ems":
    mergedcsvdir = os.path.join(pwd,file_name)
# Call the function to merge the CSV files
merge_csv_files(destination_directory,mergedcsvdir)
# Merge the CSV files end

# BEGIN prepare for ADX ingestion
# Init the schema dictionary
schema_dict = {}
#Function to create schemas in json format for the ADX loading
def create_schemas(mergedcsvdir):
    import os
    import pandas as pd
    import json
    import sys
    # Get the current working directory
    pwd = os.getcwd()
    print("current working directory: " + str(pwd))

    # Prepare schemas for ADX ingestion
    # Read the CSV file into a DataFrame
    # List all merged files in the mergedcsvdir directory
    schema_dict_table = {}
    schema_dict_mapping = {}
    files = sorted(os.listdir(os.path.join(pwd, mergedcsvdir)))
    # Load your CSV file into a DataFrame
    for file in files:

        dfs = pd.read_csv(os.path.join(mergedcsvdir,file),nrows=1)
        group_name = file.rstrip('.csv')
        schema = dfs.dtypes.to_dict()
        for col, dtype in schema.items():
            if dtype == "int64":
                schema[col] = "int64"
            elif dtype == "float64":
                schema[col] = "decimal"
            else:
                schema[col] = "string"

        schema_dict_table[group_name] = schema

        df = pd.read_csv(os.path.join(mergedcsvdir,file),nrows=10)

        # Create schemas in json format
        # Define a function to convert column datatype to the desired format
        def convert_datatype(dtype):
            if dtype == "int64":
                return "int64"
            elif dtype == "float64":
                return "decimal"
            elif dtype == "object":
                return "string"
            else:
                return dtype

        # Create a list to store column information
        columns_info = []
        # Iterate over columns of DataFrame
        for i, col in enumerate(df.columns):
            col_info = {
                "Name": col,
                "datatype": convert_datatype(df[col].dtype.name),
                "Ordinal": i
            }
            columns_info.append(col_info)

        # Append to schema dictionary
        schema_dict_mapping[group_name] = columns_info

        # Print the result in the desired format
        # for col_info in columns_info:
        #    print(col_info)
        print("Successfully created schema for the group: " + group_name )
    return schema_dict_table, schema_dict_mapping

SCHEMA = create_schemas(mergedcsvdir)
SCHEMA_DICT_TABLE = SCHEMA[0]
SCHEMA_DICT_MAPPING = SCHEMA[1]

# END Create schemas in json format
# BEGIN get SP secret from Key Vault
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.mgmt.kusto import KustoManagementClient
from azure.mgmt.kusto.models import ReadWriteDatabase

# Define Azure Key Vault parameters
KEYVAULT_NAME = "pmstats-keyvault"
SECRET_NAME = "PMSTATS01CLIENTSECRET"

# Use DefaultAzureCredential to authenticate with Azure
credential = DefaultAzureCredential()

# Create a secret client
secret_client = SecretClient(vault_url=f"https://{KEYVAULT_NAME}.vault.azure.net/", credential=credential)

# Fetch the secret from Key Vault
client_secret = secret_client.get_secret(SECRET_NAME).value

# Now you can use client_secret in your code
# Remember to remove the SPSECRET environment variable usage

# END get SP secret from Key Vault
# Your existing code continues from here...

# BEGIN ADX datebase creation
# Define Azure Data Explorer connection parameters
SUBSCRIPTION_ID = "ecfc8b89-b010-4b0a-b9e7-8e0283916908"
AAD_TENANT_ID = "72f988bf-86f1-41af-91ab-2d7cd011db47"
CLIENT_ID = "13abff5e-6038-4bb0-9fbe-4711903f1544"
#CLIENT_SECRET=os.environ.get("SPSECRET")
CLIENT_SECRET=client_secret
KUSTO_URI = "https://aiops06adx.eastus.kusto.windows.net"
KUSTO_INGEST_URI = "https://ingest-aiops06adx.eastus.kusto.windows.net/"

subscription_id = SUBSCRIPTION_ID
#credentials = ServicePrincipalCredentials(
#    client_id=CLIENT_ID,
#    secret=CLIENT_SECRET,
#    tenant=AAD_TENANT_ID
#)

credentials = ClientSecretCredential(
    tenant_id=AAD_TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)

location = 'East US'
region = 'eastus'
resource_group_name = 'aiops06-storage-resgrp'
cluster_name = 'aiops06adx'
soft_delete_period = timedelta(days=180)
hot_cache_period = timedelta(days=90)
database_name = KUSTO_DATABASE
def create_adx_database(credentials, subscription_id, resource_group_name, cluster_name, database_name, location, soft_delete_period, hot_cache_period):
    import sys
    import time
    import json
    from azure.mgmt.kusto import KustoManagementClient
    from azure.mgmt.kusto.models import ReadWriteDatabase
    from azure.common.credentials import ServicePrincipalCredentials

    kusto_management_client = KustoManagementClient(credentials, subscription_id)

    database_operations = kusto_management_client.databases
    database_exists = any(db.name == cluster_name+"/"+database_name for db in database_operations.list_by_cluster(resource_group_name, cluster_name))
    # List all databases in the cluster
    print("_________________________________________________________________")
    print("Existing databases in the cluster: " + cluster_name)
    for db in database_operations.list_by_cluster(resource_group_name, cluster_name):
        print("DATABASE NAME: " + db.name)
    print("_________________________________________________________________")
    if database_exists:
        print(f"ADX Database {database_name} already exists.")
    else:
        print(f"ADX Database {database_name} does not exist. Creating it now...")
        database = ReadWriteDatabase(location=location,
            soft_delete_period=soft_delete_period,
            hot_cache_period=hot_cache_period)

        poller = database_operations.begin_create_or_update(resource_group_name = resource_group_name, 
                                                            cluster_name = cluster_name, 
                                                            database_name = database_name,
                                                            parameters = database)
        poller.wait()
        print("ADX Database " + KUSTO_DATABASE + " created successfully")

create_adx_database(credentials, subscription_id, resource_group_name, cluster_name, database_name, location, soft_delete_period, hot_cache_period) 
#END ADX datebase creation

#BEGIN ADX database add permissions

# Add support security group to the database
principal_assignment_name = "AFO_AOI"
#User email, application ID, or security group name
principal_id = "afo-aoi@microsoft.com"
#AllDatabasesAdmin, AllDatabasesMonitor or AllDatabasesViewer
role = "Admin"
tenant_id_for_principal = AAD_TENANT_ID
#User, App, or Group
principal_type = "Group"
def add_permissions_to_adx_database(credentials, subscription_id, resource_group_name, cluster_name, database_name, principal_assignment_name, principal_id, role, tenant_id_for_principal, principal_type):
    import sys
    import time
    import json
    from azure.mgmt.kusto import KustoManagementClient
    from azure.mgmt.kusto.models import DatabasePrincipalAssignment
    from azure.common.credentials import ServicePrincipalCredentials

    kusto_management_client = KustoManagementClient(credentials, subscription_id)

    #Returns an instance of LROPoller, check https://learn.microsoft.com/python/api/msrest/msrest.polling.lropoller?view=azure-python
    try:
        poller = kusto_management_client.database_principal_assignments.begin_create_or_update(resource_group_name=resource_group_name, 
                                                                                        cluster_name=cluster_name, 
                                                                                        database_name=database_name, 
                                                                                        principal_assignment_name= principal_assignment_name, 
                                                                                        parameters=DatabasePrincipalAssignment(principal_id=principal_id, role=role, tenant_id=tenant_id_for_principal, principal_type=principal_type))
        print("ADX Database " + KUSTO_DATABASE + " updated service principal AFO_AOI <afo-aoi@microsoft.com> successfully")
    except Exception as e:
        if "Cannot add PrincipalAssignment resource 'AFO_AOI'. A PrincipalAssignment resource (name: 'cceb72c9-2921-43f6-b5c9-10ae9c5ee1f8') already exists with the same role and principal id" in str(e):
            print("Info: Service Principal with name AFO_AOI already assigned to database: " + KUSTO_DATABASE)
            print("_________________________________________________________________")
            pass       
        else:
            print("Error: ", e)
            # Handle the error appropriately, e.g., log the error, retry the operation, or raise it again
            sys.exit(0)
add_permissions_to_adx_database(credentials, subscription_id, resource_group_name, cluster_name, database_name, principal_assignment_name, principal_id, role, tenant_id_for_principal, principal_type)
#END ADX database add permissions

#BEGIN load data to ADX
def load_data_to_adx(KUSTO_INGEST_URI, KUSTO_URI, KUSTO_DATABASE, SCHEMA_DICT_TABLE, SCHEMA_DICT_MAPPING, mergedcsvdir, CLIENT_ID, CLIENT_SECRET, AAD_TENANT_ID):
    import os
    import sys
    import time
    import json
    import re
    import shutil
    import warnings
    import pandas as pd
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    from azure.kusto.data.exceptions import KustoServiceError
    from azure.kusto.data.helpers import dataframe_from_result_table
    from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, ReportLevel, ReportMethod
    from azure.kusto.data import DataFormat
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    from azure.kusto.data.exceptions import KustoServiceError
    from azure.kusto.data.helpers import dataframe_from_result_table
    from azure.identity import ClientSecretCredential
    from azure.mgmt.kusto import KustoManagementClient
    from azure.mgmt.kusto.models import ReadWriteDatabase
    from azure.mgmt.kusto.models import DatabasePrincipalAssignment
    from azure.common.credentials import ServicePrincipalCredentials
    from datetime import timedelta

    for group in SCHEMA_DICT_TABLE:

        # Create connection string
        #KCSB_INGEST = KustoConnectionStringBuilder.with_interactive_login(KUSTO_INGEST_URI)
        KCSB_INGEST = KustoConnectionStringBuilder.with_aad_application_key_authentication(KUSTO_INGEST_URI,CLIENT_ID,CLIENT_SECRET,AAD_TENANT_ID)

        #KCSB_DATA = KustoConnectionStringBuilder.with_interactive_login(KUSTO_URI)
        KCSB_DATA = KustoConnectionStringBuilder.with_aad_application_key_authentication(KUSTO_URI,CLIENT_ID,CLIENT_SECRET,AAD_TENANT_ID)

        DESTINATION_TABLE = group
        DESTINATION_TABLE_COLUMN_MAPPING = group + "_CSV_Mapping"


        # Create a client to connect to Kusto
        KUSTO_CLIENT = KustoClient(KCSB_DATA)
        
        # Create a table in the database
        CREATE_TABLE_COMMAND = ".create table " + group + " " + str(SCHEMA_DICT_TABLE[group]).replace("'", "").replace("{", "(").replace("}", ")")
        #CREATE_TABLE_COMMAND = ".create table StormEvents (StartTime: datetime, EndTime: datetime, EpisodeId: int, EventId: int, State: string, EventType: string, InjuriesDirect: int, InjuriesIndirect: int, DeathsDirect: int, DeathsIndirect: int, DamageProperty: int, DamageCrops: int, Source: string, BeginLocation: string, EndLocation: string, BeginLat: real, BeginLon: real, EndLat: real, EndLon: real, EpisodeNarrative: string, EventNarrative: string, StormSummary: dynamic)"
        #print(CREATE_TABLE_COMMAND)

        try:
            RESPONSE = KUSTO_CLIENT.execute_mgmt(KUSTO_DATABASE, CREATE_TABLE_COMMAND)
            dataframe_from_result_table(RESPONSE.primary_results[0])
            print("Table " + group + " created successfully in the database: " + KUSTO_DATABASE)
        except KustoServiceError as e:
            print("Error: ", e)
            print("Error: Check if the table already exists in the database")
            # Handle the error appropriately, e.g., log the error, retry the operation, or raise it again
            sys.exit(0)


        # Map incoming CSV data to the column names and data types used when creating the table. This maps source data fields to destination table columns
        CREATE_MAPPING_COMMAND = ".create table " + group + " ingestion csv mapping '" + group + "_CSV_Mapping' '" + str(SCHEMA_DICT_MAPPING[group]).replace("'", '"') + "'"
        #CREATE_MAPPING_COMMAND = """.create table StormEvents ingestion csv mapping 'StormEvents_CSV_Mapping' '[{"Name":"StartTime","datatype":"datetime","Ordinal":0}, {"Name":"EndTime","datatype":"datetime","Ordinal":1},{"Name":"EpisodeId","datatype":"int","Ordinal":2},{"Name":"EventId","datatype":"int","Ordinal":3},{"Name":"State","datatype":"string","Ordinal":4},{"Name":"EventType","datatype":"string","Ordinal":5},{"Name":"InjuriesDirect","datatype":"int","Ordinal":6},{"Name":"InjuriesIndirect","datatype":"int","Ordinal":7},{"Name":"DeathsDirect","datatype":"int","Ordinal":8},{"Name":"DeathsIndirect","datatype":"int","Ordinal":9},{"Name":"DamageProperty","datatype":"int","Ordinal":10},{"Name":"DamageCrops","datatype":"int","Ordinal":11},{"Name":"Source","datatype":"string","Ordinal":12},{"Name":"BeginLocation","datatype":"string","Ordinal":13},{"Name":"EndLocation","datatype":"string","Ordinal":14},{"Name":"BeginLat","datatype":"real","Ordinal":16},{"Name":"BeginLon","datatype":"real","Ordinal":17},{"Name":"EndLat","datatype":"real","Ordinal":18},{"Name":"EndLon","datatype":"real","Ordinal":19},{"Name":"EpisodeNarrative","datatype":"string","Ordinal":20},{"Name":"EventNarrative","datatype":"string","Ordinal":21},{"Name":"StormSummary","datatype":"dynamic","Ordinal":22}]'"""
        
        #print(CREATE_MAPPING_COMMAND)
        
        #sys.exit(0)


        try:
            RESPONSE = KUSTO_CLIENT.execute_mgmt(KUSTO_DATABASE, CREATE_MAPPING_COMMAND)
            dataframe_from_result_table(RESPONSE.primary_results[0])
        except KustoServiceError as e:
            # Handle the case where the CSV Mapping entity already exists
            if "already exists" in str(e):
                print("Warning: Check if the mapping already exists in the database")
                pass
        except Exception as e:
            # Handle other exceptions
            print("Error:", e)
            sys.exit(0)

        INGESTION_CLIENT = QueuedIngestClient(KCSB_INGEST)

        # All ingestion properties are documented here: https://learn.microsoft.com/azure/kusto/management/data-ingest#ingestion-properties
        INGESTION_PROPERTIES = IngestionProperties(database=KUSTO_DATABASE, table=DESTINATION_TABLE, data_format=DataFormat.CSV, ingestion_mapping_reference=DESTINATION_TABLE_COLUMN_MAPPING, additional_properties={'ignoreFirstRecord': 'true'})

        # FILE_SIZE is the raw size of the data in bytes
        #BLOB_DESCRIPTOR = BlobDescriptor(BLOB_PATH, FILE_SIZE)
        #INGESTION_CLIENT.ingest_from_blob(BLOB_DESCRIPTOR, ingestion_properties=INGESTION_PROPERTIES)
        INGESTION_CLIENT.ingest_from_file(os.path.join(mergedcsvdir, f'{group}.csv'), ingestion_properties=INGESTION_PROPERTIES)
        #print(f"{group}.csv")
        print('Done queuing up ingestion with Azure Data Explorer for PM group: ' + DESTINATION_TABLE + " in the database: " + KUSTO_DATABASE)
        print("The data will be available in the ADX with delay of 5-10 minutes")
        print("_________________________________________________________________")
        print("                                                                 ")
        #QUERY = group + " | count"
        #RESPONSE = KUSTO_CLIENT.execute_query(KUSTO_DATABASE, QUERY)
        #results_df = dataframe_from_result_table(RESPONSE.primary_results[0])
        #print(results_df)

        #sys.exit(0)
load_data_to_adx(KUSTO_INGEST_URI, KUSTO_URI, KUSTO_DATABASE, SCHEMA_DICT_TABLE, SCHEMA_DICT_MAPPING, mergedcsvdir, CLIENT_ID, CLIENT_SECRET, AAD_TENANT_ID)
#END load data to ADX
    
#create json file for ADX dashboard
import json

def replace_string_in_json(input_file, output_file, replace_string):
    with open(input_file, 'r') as file:
        data = json.load(file)
        data["title"] = replace_string
        data["dataSources"][0]["name"] = replace_string
        data["dataSources"][0]["database"] = replace_string

    with open(output_file, 'w') as file:
        json.dump(data, file, indent=4)

print("_________________________________________________________________")
print("\n")
print("All PM groups ingested successfully to ADX database: " + KUSTO_DATABASE)
print("_________________________________________________________________")
end_time = time.time()
print("\n")
#print("End time: " + str(end_time))
execution_time = end_time - start_time
print("Script execution time in seconds: " + str(execution_time))
print("_________________________________________________________________")
print("\n")
print("Follow the link to access the ADX database: "+KUSTO_URI+"/"+KUSTO_DATABASE)
print("_________________________________________________________________")
print("\n")

# Call the function with your JSON file and strings
try:
    replace_string_in_json(DASHTEMPLATE, KUSTO_DATABASE + "-dashboard-template.json", KUSTO_DATABASE)
    print("A dashboard template has been generated: " + KUSTO_DATABASE + "-dashboard-template.json")
    print("Use it to create a new dashboard in the ADX database: " + KUSTO_DATABASE)
    print("Go to " + KUSTO_URI + " and click on dashboards then select the 'Import dashboard from file' to create a new dashboard")
except Exception as e:
    print(f"Error creating dashboard template: {e}")

# END Create a new dashboard
