# Create-NCL-ICB-Medical-Locations

Created NCL ICB Medical Locations created by Donald Maruta - 20 Jul 23
Web Address: https://api.nhs.uk/service-search/search

Python code designed to be run in ArcGIS OnLine Notebook

This allows users to create all Medical Facilities within NCL ICB and export them as Web Map Services.

It carries out the following actions in the following order
 - Connect to AGOL
 - Import Required Modules
 - Initial Setup
 - Import Data from NHS Digital
 - Import CSV files to FGDB Tables
 - Merge All Tables
 - Remove Unrequired locations
 - Export Services
 - Write the resulting DataFrame to a new CSV file
 - Export Opening Times
 - Create small Opening Time files to avoid file limit size
 - Join the Tables to the main FC
 - Delete Unneeded Fields
 - Delete Null Data
 - Convert the Date Time Field
 - Initial creation of the service - All Services
 - Overwrite the existing service - All Services
 - Initial creation of the service - All Data Times
 - Overwrite the existing service - All Data Times
 - Initial creation of the service - All Data2
 - Overwrite the existing service - All Data2
 - Code to delete unnecessary file
