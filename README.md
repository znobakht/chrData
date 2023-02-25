# chrData
This project used for grouping chr data.
1- At first, you need a config folder with keys.js file with these parameters:
mongoURL: your db url
dbNameTemplate: your collections template name
finalDB: name of final database
tmpCollectionName: a temporary collection name

2- run all.js file
  this file will change the ts format at first.
  then grouping objects base four fileds
  finally, count them base time, fieldName
  
 3- run both changes files for make int values to wanted names.
 
 4- run procedures file for creating 2 collection for procedures and protocol based procedures
 
 5- run filter file for creation of filters in grafana
