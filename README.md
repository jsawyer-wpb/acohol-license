# ALCOHOL-LICENSE
Script to copy data for alcohol licenses from Community Plus to GIS-Cluster

## 1. first edit:
  * email on failure added
  * reworked workflow to append to existing feature class the new records as opposed to recreating the entire feature class

## 2. second edit:
  * cleaned up code with list comprehensions and other small tweaks
  * per discussion with Heidi, have changed update source to pull from entire business license table and then pare down the table in this script as opposed to her creating the pared down table

## 3. clean up edit:

  * cleaned up so it looks nice; print statements deleted, extraneous commented out code gone
  
  
## 4. addition of log
   *  Added a text file log written to Slades pc (must be run locally) which indicates no new alcohol licenses were found or prints out a report of the new licenses found to same log file (it is the same report emailed to slade and caroline).


