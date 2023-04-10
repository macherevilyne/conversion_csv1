A script that reads csv files from a specific directory and enters them into the My SQL database. When duplicating unique fields,
the data does not fall into the database. Unique fields (Product,Policy_Number, Subproduct). For the script to work correctly, it is need:
1)Change host, user, password, database, directory in the configuration file config.ini to the current data to connect to the database 
2)Change the table name when creating a table and when querying the database when adding data to the current data to connect to the database
3)Run script