# Импорт библиотек
import os
import pandas as pd
import mysql.connector
import configparser



# Reading connection parameters from the configuration file
config = configparser.ConfigParser()
config.read('config.ini')



# Database Connection
cnx = mysql.connector.connect(
    host=config['mysql']['host'],
    user=config['mysql']['user'],
    password=config['mysql']['password'],

)



# Getting a List of Existing Databases
cursor = cnx.cursor()
cursor.execute("SHOW DATABASES")
databases = cursor.fetchall()


# List of existing database names
existing_databases = [database[0] for database in databases]

# Database name
db_name = config['mysql']['database']


# If the required database already exists, we connect to it
if db_name in existing_databases:
    cnx = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    print('База данных {}'.format(db_name),
          "уже существует.Подключение к базе данных {} успешно выполнено".format(db_name))

# If the required database does not exist, create it and connect to it
else:
    cursor.execute("CREATE DATABASE {}".format(db_name))
    cnx = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    print("Database {} was successfully created and connected to".format(db_name))



# Creating a Table in a MySQL Database
"""CHANGING THE TABLE 22q2 TO THE CURRENT DATA TO CONNECT TO YOUR DATABASE"""

cursor = cnx.cursor()
cursor.execute(
    "CREATE TABLE IF NOT EXISTS 22q2 (Product_Group VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Product VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Subproduct VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Policy_Number VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Gender VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Age VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Joint_Life VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Start_Date VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Lapse_Date VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "End_Date VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Annual_Premium VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Single_Premium VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Premium_Indexation VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Changing_Premium VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Fonds_Value VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "PVPrem VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "PVFP VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "PVCost VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "PVSCR VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Statutory_Res VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "SII_Res VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Death_Benefit_Sum VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Sum_of_Salary VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Disability_Annuity VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Benefit_Indexation VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Waiting_Period VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Coverage_Style VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Benefit_Duration VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "NBM VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "CoC VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "Combined_Ratio VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
    "UNIQUE KEY unique_policy_product (Product,Policy_Number, Subproduct))")

#  Save changes to the database

cnx.commit()



"""CHANGING NAME TABLE:22q2 in INSERT INTO sql TO THE CURRENT DATA TO CONNECT TO YOUR DATABASE"""

# Retrieving a list of CSV files from a specified directory
directory = config['mysql']['directory']
files = os.listdir(directory)
csv_files = [f for f in files if f.endswith('.csv')]

# Read CSV files and write data to MySQL database
for file in csv_files:
    file_path = os.path.join(directory, file)
    df = pd.read_csv(file_path, low_memory=False)
    df = df.where(pd.notnull(df), '')
    print(f"TOTAL RECORDS TO BE MADE FROM DATAFRAME TO DATABASE:", len(df))

    records_inserted = 0
    try:
        for index, row in df.iterrows():
            cursor = cnx.cursor()
            sql = "INSERT INTO 22q2 (Product_Group, Product, Subproduct, Policy_Number, Status, Gender, Age, Joint_Life," \
                  " Start_Date, Lapse_Date, End_Date, Annual_Premium, Single_Premium, Premium_Indexation, Changing_Premium, " \
                  "Fonds_Value, PVPrem, PVFP, PVCost, PVSCR, Statutory_Res, SII_Res, Death_Benefit_Sum, Sum_of_Salary, " \
                  "Disability_Annuity, Benefit_Indexation, Waiting_Period, Coverage_Style, Benefit_Duration, NBM, CoC, " \
                  "Combined_Ratio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                  "%s,%s,%s,%s,%s,%s)"
            val = tuple(row)
            cursor.execute(sql, val)
            records_inserted += cursor.rowcount
    except mysql.connector.IntegrityError as error:
        if error.errno == 1062:  # код ошибки дубликата ключа
            print(f'THERE ARE DUPLICATE RECORDS ON DOWNLOAD, NO DUPLICATES WILL BE ADDED')
        else:
            print(f"Error: {error}")
    print(f"TOTAL RECORDS MADE TO DATABASE: {records_inserted}")

# Close the cursor and save the database
cnx.commit()
cursor.close()
cnx.close()
