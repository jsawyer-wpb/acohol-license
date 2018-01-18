# alcohol-license
Script to copy new applications for alcohol licenses from Community Plus to GIS-Cluster

first edit:

-email on failure added
-reworked workflow to append to existing feature class the new records as opposed to recreating the entire feature class

second edit:

-cleaned up code with list comprehensions and other small tweaks
-per discussion with Heidi, have changed update source to pull from entire business license table and then pare down the table in this script as opposed to her creating the pared down table
