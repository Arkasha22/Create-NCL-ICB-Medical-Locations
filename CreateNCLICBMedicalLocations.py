##APIs Ingestion file created by Donald Maruta, Senior Geospatial Manager, NCL ICB, 20 Jul 23

#Web Address: https://api.nhs.uk/service-search/search

from arcgis.gis import GIS
gis = GIS("home")

#Import Required Modules
import sys, arcpy, arcgis, os, tempfile, csv, requests, time, json, ast, zipfile, glob
import pandas as pd
from urllib import request
from datetime import date
from arcgis.gis import GIS
from arcgis import features
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection
arcpy.env.overwriteOutput=True #This allows data to be overwritten
todayDate = datetime.datetime.now().strftime("%y%m%d%y%H%M")
print(todayDate)

#Initial Setup
arcpy.env.workspace = "/arcgis/home/"
out_folder_path = "/arcgis/home/"
out_name = 'CSV_Ingest_' + todayDate + '.gdb'
out_data = "AllData"
out_data_FC = "AllDataFC"
out_data_FC2 = "TempAllData"
FGDBpath = '/arcgis/home/CSV_Ingest_' + todayDate + '.gdb'

#List of datasets
datasets = [
    {
        'filter': "OrganisationTypeID eq 'SCL'", #Care homes and care at home
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
    {
        'filter': "OrganisationTypeID eq 'CLI'", #Clinics
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
    {
        'filter': "OrganisationTypeID eq 'DEN'", #Dentists
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
    {
        'filter': "OrganisationTypeID eq 'GPB'", #GPs
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
    {
        'filter': "OrganisationTypeID eq 'HOS'", #Hospitals
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
    {
        'filter': "OrganisationTypeID eq 'MIU'", #Minor Injury Units
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
    {
        'filter': "OrganisationTypeID eq 'OPT'", #Opticians
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
    {
        'filter': "OrganisationTypeID eq 'PHA'", #Pharmacies
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
#Add more datasets as needed
]

#Specify the file paths where you want to save the CSV files
csv_file_paths = [
    "/arcgis/home/SCL.csv",
    "/arcgis/home/CLI.csv",
    "/arcgis/home/DEN.csv",
    "/arcgis/home/GPB.csv",
    "/arcgis/home/HOS.csv",
    "/arcgis/home/MIU.csv",
    "/arcgis/home/OPT.csv",
    "/arcgis/home/PHA.csv",
    #Add more file paths as needed
]

for dataset, csv_file_path in zip(datasets, csv_file_paths):
    response = requests.request(
        method='POST',
        url='https://api.nhs.uk/service-search/search?api-version=1',
        headers={
            'Content-Type': 'application/json',
            'subscription-key': '557d555fd712449f894e78e50a460000'
        },
        json=dataset
    )

    #Parse the response as JSON
    data = response.json()

    #Extract the required data from the JSON response
    output = []
    for item in data.get('value', []):
        output.append([
            item.get('OrganisationID'),
            item.get('NACSCode'),
            item.get('OrganisationName'),
            item.get('OrganisationType'),
            item.get('Postcode'),
            item.get('URL'),        
            item.get('Address1'),
            item.get('Address2'),
            item.get('Address3'),
            item.get('City'),
            item.get('County'),
            item.get('Latitude'),
            item.get('Longitude'),
            item.get('ServicesProvided'),
            item.get('OpeningTimes'),
            item.get('Contacts'),
            item.get('LastUpdatedDate'),
        ])

    #Open the CSV file in write mode
    with open(csv_file_path, 'w', newline='') as csvfile:
        #Create a CSV writer object
        csv_writer = csv.writer(csvfile)

        #Write the header row
        csv_writer.writerow(['OrganisationID', 'OCS_Code', 'OrganisationName', 'OrganisationType', 'Postcode', 'URL', 'Address1', 'Address2', 'Address3', 'City', 'County', 'Latitude', 'Longitude', 'ServicesProvided', 'OpeningTimes', 'Contacts', 'LastUpdatedDate'])

        #Write the output to the CSV file
        csv_writer.writerows(output)

    #Confirmation message
    print(f"Output saved as CSV: {csv_file_path}")

    #Load the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path, encoding = "cp1252")

    #Create a new column to store the extracted phone numbers
    df['PhoneNumber'] = ""

    #Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        contacts_data = row['Contacts']
    
        #Check if the value is NaN (not a string)
        if isinstance(contacts_data, str):
            #Parse the JSON data
            parsed_data = json.loads(contacts_data)

            #Extract the telephone number
            telephone_number = parsed_data[0]["OrganisationContactValue"]

            #Update the "PhoneNumber" column with the extracted telephone number
            df.at[index, 'PhoneNumber'] = telephone_number

    #Drop the original "Contacts" column
    df.drop(columns=['Contacts'], inplace=True)

    #Save the modified DataFrame back to a CSV file
    df.to_csv(csv_file_path, index=False)

#Confirmation message
print("All datasets processed successfully.")

#Create File GDB
arcpy.CreateFileGDB_management(out_folder_path, out_name)

#List of all files
filelist = ['SCL', 'CLI', 'DEN', 'GPB', 'HOS', 'MIU', 'OPT', 'PHA']
length = len(filelist)

#Import CSV files to FGDB Tables
for i in range(length):
    input_table = "/arcgis/home/"+(filelist[i])+".csv"
    arcpy.conversion.TableToGeodatabase(input_table, out_name)

#Merge All Tables
arcpy.env.workspace = FGDBpath
arcpy.management.Merge(filelist, out_data)

#Delete Non Required Tables
for i in range(length):
    input_table = filelist[i]
    print(input_table)
    arcpy.management.Delete(input_table)

#Remove Unrequired locations
arcpy.conversion.TableToGeodatabase("/arcgis/home/PostCodeLookup_V3.csv", FGDBpath)
tempdata = arcpy.management.AddJoin(out_data, "Postcode", "PostCodeLookup_V3", "PostCode", "KEEP_COMMON")
arcpy.management.CopyRows(tempdata, "AllData2")

#Export Services
#List of fields to keep
arcpy.env.workspace = FGDBpath
input_table = "AllData"
output_table = "/arcgis/home/Services.csv"
fields_to_keep = ["OrganisationID", "ServicesProvided"]

#Create a field mapping object
field_mappings = arcpy.FieldMappings()

#Get field info for the input table
input_fields = arcpy.ListFields(input_table)

#Loop through input fields and add desired fields to field mapping
for field in input_fields:
    if field.name in fields_to_keep:
        field_map = arcpy.FieldMap()
        field_map.addInputField(input_table, field.name)
        field_mappings.addFieldMap(field_map)

#Create the output table using the field mappings
sqlClause = "ServicesProvided IS NOT NULL"
arcpy.conversion.ExportTable(input_table, output_table, sqlClause, "", field_mappings)
print("Table exported successfully.")

#Read the original CSV file
df = pd.read_csv("/arcgis/home/Services.csv")
print("Data Frame Completed!")

#Create a new DataFrame to hold the transformed data
new_data = {'OrganisationID': [], 'Services': []}

#Iterate through the original DataFrame
for index, row in df.iterrows():
    org_id = row['OrganisationID']
    services = row['ServicesProvided'].strip('[]').replace("'", "").split(',')
    print(org_id)
    print(services)
    for service in services:
        new_data['OrganisationID'].append(org_id)
        new_data['Services'].append(service)

#Create the new DataFrame
new_df = pd.DataFrame(new_data)

#Write the resulting DataFrame to a new CSV file
new_df.to_csv("/arcgis/home/Services2.csv", index=False)

#Export Opening Times
#List of fields to keep
arcpy.env.workspace = FGDBpath
input_table = "AllData"
output_table = "/arcgis/home/OpeningTimes.csv"
fields_to_keep = ["OrganisationID", "OrganisationType", "OpeningTimes"]

#Create a field mapping object
field_mappings = arcpy.FieldMappings()

#Get field info for the input table
input_fields = arcpy.ListFields(input_table)

#Loop through input fields and add desired fields to field mapping
for field in input_fields:
    if field.name in fields_to_keep:
        field_map = arcpy.FieldMap()
        field_map.addInputField(input_table, field.name)
        field_mappings.addFieldMap(field_map)

#Create the output table using the field mappings
sqlClause = "OpeningTimes IS NOT NULL"
arcpy.conversion.ExportTable(input_table, output_table, sqlClause, "", field_mappings)
print("Table exported successfully.")

#Create small Opening Time files to avoid file limit size
rows = pd.read_csv("/arcgis/home/OpeningTimes.csv", chunksize=500)
for i, chuck in enumerate(rows):
    chuck.to_csv('/arcgis/home/OT{}.csv'.format(i)) #i is for chunk number of each iteration

##Loop through Opening Times

OTlist = ["OT0.csv", "OT1.csv", "OT2.csv", "OT3.csv", "OT4.csv", "OT5.csv", "OT6.csv", "OT7.csv"]
length = len(OTlist)

for i in range(length):
    #Read the original CSV file
    df = pd.read_csv("/arcgis/home/"+OTlist[i])
    print("Data Frame Completed!")

    #Create a new DataFrame to hold the transformed data
    new_data = {'OrganisationID': [], 'OrganisationType': [], 'OpeningTimes': []}

    for index, row in df.iterrows():
        org_id = row['OrganisationID']
        org_type = row['OrganisationType']
        services = row['OpeningTimes'].strip('[]').replace("{", "").replace("}", "").replace("'", "").split('true,')
        print(org_id)
        print(services)
        for service in services:
            new_data['OrganisationID'].append(org_id)
            new_data['OrganisationType'].append(org_type)
            new_data['OpeningTimes'].append(service)

    #Create the new DataFrame
    new_df = pd.DataFrame(new_data)

    #Write the resulting DataFrame to a new CSV file
    new_df.to_csv("/arcgis/home/"+OTlist[i], index=False)
    
    #Upload to FGDB
    arcpy.conversion.TableToGeodatabase("/arcgis/home/"+OTlist[i], FGDBpath)

#Merge All Tables
arcpy.env.workspace = FGDBpath
listOT = arcpy.ListTables("OT*")
arcpy.management.Merge(listOT, "OT_All")

#Import Services CSV into FGDB
arcpy.conversion.TableToGeodatabase("/arcgis/home/Services2.csv", FGDBpath)

#Join the Tables to the main FC
#First the Opening Times
arcpy.env.workspace = FGDBpath
TempDataTimes = arcpy.management.AddJoin("AllData2", "AllData_OrganisationID", "OT_All", "OrganisationID")
arcpy.management.CopyRows(TempDataTimes, "AllDataTimes")

#Now the Services
arcpy.env.workspace = FGDBpath
TempServices = arcpy.management.AddJoin("AllData2", "AllData_OrganisationID", "Services2", "OrganisationID")
arcpy.management.CopyRows(TempServices, "AllServices")

#Delete the unneeded fields for Services
kpFlds = ["AllData2_AllData_OrganisationID", "AllData2_AllData_OrganisationName", "AllData2_AllData_PhoneNumber", "AllData2_AllData_Latitude", "AllData2_AllData_Longitude", "Services2_Services", 
         "AllData2_PostCodeLookup_V3_LSOA21CD", "AllData2_PostCodeLookup_V3_MSOA21CD", "AllData2_PostCodeLookup_V3_LTLA22NM", "AllData2_PostCodeLookup_V3_UTLA22NM"]
arcpy.management.DeleteField("AllServices", kpFlds, "KEEP_FIELDS")

#Delete the unneeded fields for OpeningTimes
kpFlds = ["AllData2_AllData_OrganisationID", "AllData2_AllData_OrganisationType", "AllData2_AllData_OrganisationName", "AllData2_AllData_PhoneNumber", "AllData2_AllData_Latitude", "AllData2_AllData_Longitude", "OT_All_OpeningTimes", 
         "AllData2_PostCodeLookup_V3_LSOA21CD", "AllData2_PostCodeLookup_V3_MSOA21CD", "AllData2_PostCodeLookup_V3_LTLA22NM", "AllData2_PostCodeLookup_V3_UTLA22NM"]
arcpy.management.DeleteField("AllDataTimes", kpFlds, "KEEP_FIELDS")

#Delete the unneeded fields for AllDataFC2
delFlds = ["AllData_ServicesProvided", "AllData_OpeningTimes"]
arcpy.management.DeleteField("AllData2", delFlds)

#Delete Null Data
tempData = arcpy.management.SelectLayerByAttribute("AllDataTimes", "", "OT_All_OpeningTimes IS NULL")
arcpy.management.DeleteRows(tempData)
tempData = arcpy.management.SelectLayerByAttribute("AllServices", "", "Services2_Services IS NULL")
arcpy.management.DeleteRows(tempData)

#Convert the Date Time Field
arcpy.management.AddField("AllDataTimes", "Day", "TEXT", "", "", 3, "Day")
arcpy.management.AddField("AllDataTimes", "StartTime", "TEXT", "", "", 4, "StartTime")
arcpy.management.AddField("AllDataTimes", "EndTime", "TEXT", "", "", 4, "EndTime")
expDay = "Mid($feature.OT_All_OpeningTimes, 11,3)"
arcpy.management.CalculateField("AllDataTimes", "Day", expDay, "ARCADE")
expST = "Mid($feature.OT_All_OpeningTimes,Find('-', $feature.OT_All_OpeningTimes) - 5, 2) + Mid($feature.OT_All_OpeningTimes,Find('-', $feature.OT_All_OpeningTimes) - 2, 2)"
arcpy.management.CalculateField("AllDataTimes", "StartTime", expST, "ARCADE")
expET = "Mid($feature.OT_All_OpeningTimes,Find('-', $feature.OT_All_OpeningTimes) +1, 2) + Mid($feature.OT_All_OpeningTimes,Find('-', $feature.OT_All_OpeningTimes) +4, 2)"
arcpy.management.CalculateField("AllDataTimes", "EndTime", expET, "ARCADE")
arcpy.management.DeleteField("AllDataTimes", "OT_All_OpeningTimes")

arcpy.conversion.ExportTable("AllServices", "AllServices.csv")
arcpy.conversion.ExportTable("AllDataTimes", "AllDataTimes.csv")
arcpy.conversion.ExportTable("AllData2", "AllData2.csv")

#Initial creation of the service - All Services
#my_csv = ("/arcgis/home/AllServices.csv")
#item_prop = {'title':'AllServices'}
#csv_item = gis.content.add(item_properties=item_prop, data=my_csv)
#params={"type": "csv", "locationType": "coordinates", "latitudeFieldName": "AllData2_AllData_Latitude", "longitudeFieldName": "AllData2_AllData_Longitude"}
#csv_item.publish(publish_parameters=params)
#csv_item.publish(overwrite = True)

#Overwrite the existing service - All Services
feat_id = '1e3ecd54f5b04a44934cf20d23d38c76'
item = gis.content.get(feat_id)
item_collection = FeatureLayerCollection.fromitem(item)
#call the overwrite() method which can be accessed using the manager property
item_collection.manager.overwrite('/arcgis/home/AllServices.csv')
#arcpy.management.DeleteRows(item)
item.share(everyone=True)
update_dict = {"capabilities": "Query,Extract"}
item_collection.manager.update_definition(update_dict)
item.content_status="authoritative"

#Initial creation of the service - All Data Times
#my_csv = ("/arcgis/home/AllDataTimes.csv")
#item_prop = {'title':'AllDataTimes'}
#csv_item = gis.content.add(item_properties=item_prop, data=my_csv)
#params={"type": "csv", "locationType": "coordinates", "latitudeFieldName": "AllData2_AllData_Latitude", "longitudeFieldName": "AllData2_AllData_Longitude"}
#csv_item.publish(publish_parameters=params)
#csv_item.publish(overwrite = True)

#Overwrite the existing service - All Data Times
feat_id = 'bdd4edb63e8241d3a8eb093b2a52bd78'
item = gis.content.get(feat_id)
item_collection = FeatureLayerCollection.fromitem(item)
#call the overwrite() method which can be accessed using the manager property
item_collection.manager.overwrite('/arcgis/home/AllDataTimes.csv')
item.share(everyone=True)
update_dict = {"capabilities": "Query,Extract"}
item_collection.manager.update_definition(update_dict)
item.content_status="authoritative"

#Initial creation of the service - All Data 2
#my_csv = ("/arcgis/home/AllData2.csv")
#item_prop = {'title':'AllData2'}
#csv_item = gis.content.add(item_properties=item_prop, data=my_csv)
#params={"type": "csv", "locationType": "coordinates", "latitudeFieldName": "AllData_Latitude", "longitudeFieldName": "AllData_Longitude"}
#csv_item.publish(publish_parameters=params)
#csv_item.publish(overwrite = True)

#Overwrite the existing service - All Data 2
feat_id = '24aad5f2368a4ef9beca0845d427137b'
item = gis.content.get(feat_id)
item_collection = FeatureLayerCollection.fromitem(item)
#call the overwrite() method which can be accessed using the manager property
item_collection.manager.overwrite('/arcgis/home/AllData2.csv')
item.share(everyone=True)
update_dict = {"capabilities": "Query,Extract"}
item_collection.manager.update_definition(update_dict)
item.content_status="authoritative"

#Code to delete unnecessary files
#List of files to preserve
files_to_preserve = ["PostCodeLookup_V3.csv"]

#Get a list of all files in the directory
all_files = glob.glob(os.path.join(out_folder_path, "*"))

#Iterate over each file
for file_path in all_files:
    try:
        #Get the file name
        file_name = os.path.basename(file_path)
    
        #Check if the file name is not in the list of files to preserve
        if file_name not in files_to_preserve:
            #Delete the file
            os.remove(file_path)
            print(f"Deleted {file_name}")
    except:
        pass

print("All files except the specified ones have been deleted.")