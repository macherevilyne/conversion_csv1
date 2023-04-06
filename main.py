# Импорт библиотек
import os
import pandas as pd
import mysql.connector

"""CHANGING db_name TO THE CURRENT DATA TO CONNECT TO YOUR DATABASE"""
# Название базы данных, к которой вы хотите подключиться
db_name = "csv"

"""CHANGING HOST, PORT, USER, PASSWORD TO THE CURRENT DATA TO CONNECT TO YOUR DATABASE"""
# Установка соединения с базой данных MySQL
mydb = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="admin",
    password="11111111Qwe`")

# Получение списка существующих баз данных
cursor = mydb.cursor()
cursor.execute("SHOW DATABASES")
databases = cursor.fetchall()
# Список имен существующих баз данных
existing_databases = [database[0] for database in databases]


"""CHANGING HOST, PORT, USER, PASSWORD TO THE CURRENT DATA TO CONNECT TO YOUR DATABASE"""

# Если нужная база данных уже существует, подключаемся к ней
if db_name in existing_databases:
    mydb = mysql.connector.connect(
        host="localhost",
        port=3306,
        user="admin",
        password="11111111Qwe`",
        database=db_name
    )
    print('База данных {}'.format(db_name),
          "уже существует.Подключение к базе данных {} успешно выполнено".format(db_name))

# Если нужная база данных не существует, создаем ее и подключаемся к ней
else:
    cursor.execute("CREATE DATABASE {}".format(db_name))
    mydb = mysql.connector.connect(
        host="localhost",
        port=3306,
        user="admin",
        password="11111111Qwe`",
        database=db_name
    )

    print("Баз данных {} успешно создана и к ней выполнено подключение ".format(db_name))

# Создание таблицы в базе данных MySQL
"""CHANGING THE TABLE TO THE CURRENT DATA TO CONNECT TO YOUR DATABASE"""

cursor = mydb.cursor()
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

#  Закрытие курсора и сохранение изменений в базе данных
cursor.close()
mydb.commit()

# Создание курсора
cursor = mydb.cursor()

"""YOU NEED TO CHANGE THE DIRECTORY VARIABLE TO THE CURRENT DIRECTORY (PATH) WHERE THE CSV FILES WILL BE READ FROM """

# Получение списка файлов CSV из заданной директории
directory = r'h:\IT\First_project'
files = os.listdir(directory)
csv_files = [f for f in files if f.endswith('.csv')]

# Чтение файлов CSV и запись данных в базу данных MySQL
for file in csv_files:
    file_path = os.path.join(directory, file)
    df = pd.read_csv(file_path, low_memory=False)
    df = df.where(pd.notnull(df), '')
    print(f"ВСЕГО ЗАПИСЕЙ НУЖНО СДЕЛАТЬ ИЗ DATAFRAME В БАЗУ ДАННЫХ:", len(df))

    records_inserted = 0
    try:
        for index, row in df.iterrows():
            cursor = mydb.cursor()
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
            print(f'ПРИ ЗАГРУЗКЕ ЕСТЬ ДУБЛИКАТЫ ЗАПИСЕЙ, ДУБЛИКАТЫ НЕ БУДУТ ДОБАВЛЕНЫ')
        else:
            print(f"Ошибка: {error}")
    print(f"ВСЕГО ЗАПИСЕЙ СДЕЛАНО В БАЗУ ДАННЫХ: {records_inserted}")

# Закрытие курсора и базы данных
mydb.commit()
cursor.close()
mydb.close()
