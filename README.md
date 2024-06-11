# csvtoadx

To address the need of the AFO support team to process and visualize PM stats a python script has been developed to load the data to the Azure Data Explorer (ADX) and create Dashboard templates. The script does following:

Untar the PM stats or TechSupport tarballname.tar.gz to a subdirectory with the same name but stripped “.tar.gz” →<tarballname>
Please be aware that only tarballname.tar.gz format is working for now.
Merge the PM stats raw files in each group resulting in one file per group
Create the ADX database where the data will be loaded to
Create all tables based on the list of groups extracted from the raw data
Create the schema based on the merged files in each group/table
Load the data into ADX
Add access permissions to the security group (SG) “AFO_AOI<afo-aoi@microsoft.com>” Please check here if you are member of the SG My Security Group Memberships (microsoft.com) If not please drop me a message to add you. Without being a member you will not be able to access the data. 
Create a dashboard template <DBNAME>-dashboard-template.json that you can import to ADX. https://learn.microsoft.com/en-us/azure/data-explorer/azure-data-explorer-dashboards#to-create-new-dashboard-from-a-file
Clean up the untarred files but leaves the merged files in the pwd/tarballname/
 

Supported CSV data types
EMS processed MCC and MME data
MCC TechSupport
 

Installation steps
Following steps will be run only once to setup the environment

 

Install required python libraries to run the script
Login to logstore and run following command

python3 /data/share/importlibs.py
 

Stable version of the script mergeCSVpushtoADX.py
You can run the script from the logstore shared location /data/share or copy it locally at your convenience

 

How to execute the script
Login to logstore 
cd /localdata/anysubdir/ 

# This ensures the data files will be processed on a local disk and be much faster than on any network mounted volume like /logstore/

Execute> python3 /data/share/mergeCSVpushtoADX.py -f /logstore/ticketnumber/tarball.tar.gz -d TESTDB -s ems|ts -t /path/mydashtemplate.json
-h help
-f tarball file to be processed
-d Target ADX Database to push the data to
-s ems|ts specify the source of the data (optional)
-t template, specify a dashboard template json file (optional)
The execution of data about 50MB takes ~ 10 min. Allow additional 5-10 min until the ADX completes the ingestion.
Download the dashboard template <DBNAME>-dashboard-template.json
Go to ADX cluster https://aiops06adx.eastus.kusto.windows.net
Import the dashboard template https://learn.microsoft.com/en-us/azure/data-explorer/azure-data-explorer-dashboards#to-create-new-dashboard-from-a-file
Clean up your files
Happy data plotting
 

Example
ttavlari@sus-ttavlari:~$ cd /localdata/adxtest
ttavlari@sus-ttavlari:/localdata/adxtest$ python3 /data/share/mergeCSVpushtoADX.py -f /logstore/1995418/an3000-mcm-slot8cpu1-TechSupportInfo-04-04-24-1712215793.tar.gz -d TESTDBX -s ts -t MYDASH.json
 
 

Additional resources:
What is Azure Data Explorer? - Azure Data Explorer | Microsoft Learn

Kusto Query Language (KQL) overview - Azure Data Explorer & Real-Time Analytics | Microsoft Learn

Kusto | Kusto

https://aka.ms/ke <<< get the Kusto desktop client

AIOPS (sharepoint.com)

 

The script has been integrated also with the SupportInfoAnalyzer to allow CSV upload to ADX with one click

Using SupportInfo Analyzer (sharepoint.com)
