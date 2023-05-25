# Importing modules
from django.utils import timezone
from django.shortcuts import render, redirect
from django.core.files.storage import FileSystemStorage
from .forms import UploadFileForm
from django.urls import reverse
import os
import pandas as pd
import mysql.connector
import configparser
from .models import UploadedFile



"""A function that opens a connection to a MySQL database using the parameters specified in the Django settings. 
Creates a table if it does not exist. Next comes the file upload form, if the form is valid when uploading files and it does not exist in the path that is specified, 
saves the file. Next, CSV files are read from the specified directory and data is written to the MySQL database."""


def connect_use_db(request):


    # Reading connection parameters from the configuration file
    config = configparser.ConfigParser()
    config.read('config_conversion_csv.ini')

    # connect to server

    cnx = mysql.connector.connect(
        host= config['conversion_csv']['HOST'],
        user=config['conversion_csv']['USER'],
        password=config['conversion_csv']['PASSWORD'],

    )


    # Create cursor
    cursor = cnx.cursor()


    # Checking the existence of the database, if the database does not exist, then creates a database and connects if it exists, then connects

    cursor.execute("SHOW DATABASES")
    databases = [database[0] for database in cursor]

    db_name = config['conversion_csv']['DATABASE']

    if db_name not in databases:
        cursor.execute(f"CREATE DATABASE {db_name}")
        cnx = mysql.connector.connect(
            host=config['conversion_csv']['HOST'],
            user=config['conversion_csv']['USER'],
            password=config['conversion_csv']['PASSWORD'],
            database=config['conversion_csv']['DATABASE']
        )

        print(f"База данных {db_name} создана.")
    else:
        cnx = mysql.connector.connect(
            host=config['conversion_csv']['HOST'],
            user=config['conversion_csv']['USER'],
            password=config['conversion_csv']['PASSWORD'],
            database=config['conversion_csv']['DATABASE']
        )
        print(f"База данных {db_name} уже существует.")




    # form dowload files


    if request.method == 'POST':

        form = UploadFileForm(request.POST, request.FILES)

        if form.is_valid():
            uploaded_file = request.FILES['file']
            fs = FileSystemStorage()

            # Retrieving a list of CSV files from a specified directory
            path = 'h:\IT\First_project'
            os.makedirs(path, exist_ok=True) # создание папки если нет

            if not os.path.isfile(os.path.join(path, uploaded_file.name)):
                fs.save(uploaded_file.name, uploaded_file)


            try:

                if not os.path.isdir(path):
                    raise FileNotFoundError(
                        f"Path '{path}' does not exist.Check that the path in the configuration file is correct")


            except FileNotFoundError as e:
                print(f"Error: {e}")
                exit()

            # reading and inserting data into a database

            files = os.listdir(path)

            csv_files = [f for f in files if f.endswith('.csv')]

            # Read CSV files and write data to MySQL database
            for file in csv_files:
                table_name = file.split("_")[-1].split(".")[0].lower()
                cursor = cnx.cursor()
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} (Product_Group VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
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

                file_path = os.path.join(path, file)
                last_uploaded_file = file
                df = pd.read_csv(file_path, low_memory=False)
                df = df.where(pd.notnull(df), '')

                records_inserted = 0


                try:

                    for index, row in df.iterrows():
                        cursor = cnx.cursor()

                        sql = f"INSERT INTO {table_name} (Product_Group, Product, Subproduct, Policy_Number, Status, Gender, Age, Joint_Life," \
                              " Start_Date, Lapse_Date, End_Date, Annual_Premium, Single_Premium, Premium_Indexation, Changing_Premium, " \
                              "Fonds_Value, PVPrem, PVFP, PVCost, PVSCR, Statutory_Res, SII_Res, Death_Benefit_Sum, Sum_of_Salary, " \
                              "Disability_Annuity, Benefit_Indexation, Waiting_Period, Coverage_Style, Benefit_Duration, NBM, CoC, " \
                              "Combined_Ratio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                              "%s,%s,%s,%s,%s,%s)"
                        val = tuple(row)
                        cursor.execute(sql, val)
                        records_inserted += cursor.rowcount

                        # Close the cursor and save the database



                    print(f"TOTAL RECORDS MADE TO DATABASE: {records_inserted}")
                    print(f"TOTAL RECORDS TO BE MADE FROM DATAFRAME TO DATABASE:", len(df))
                    cnx.commit()
                    cursor.close()
                    cnx.close()
                    file_upload = UploadedFile.objects.create(
                        user=request.user,
                        file=uploaded_file.name,
                        uploaded_at=timezone.now()

                    )
                    file_upload.save()

                    return redirect(
                            reverse('upload_success') + f'?uploaded_file={uploaded_file}&records_inserted={records_inserted}')



                except mysql.connector.errors.ProgrammingError as error:
                    error_msg = str(error)
                    os.remove(file_path)
                    return render(request, 'upload_error.html', {'error_msg': error_msg})

                except mysql.connector.IntegrityError as error:

                    if error.errno == 1062:  # код ошибки дубликата ключа
                        print(
                            f' Uploaded file: {last_uploaded_file} THERE ARE DUPLICATE RECORDS ON DOWNLOAD, NO DUPLICATES WILL BE ADDED')


                    else:
                        print(f"Last uploaded file: {last_uploaded_file}")
                        print(f"Error: {error}")


    else:
        form = UploadFileForm()

    context = {


        'form': form,

    }
    return render(request, 'upload_file.html', context)



"""The function executes the request if there is an error in the uploaded csv file"""


def upload_error(request):
    return render(request, 'upload_error.html')





"""This function is responsible for displaying the page that appears after successfully downloading the file and writing
data to the database."""

def upload_success(request):
    uploaded_file = request.GET.get('uploaded_file')
    records_inserted = request.GET.get('records_inserted')
    context = {
        'uploaded_file': uploaded_file,
        'records_inserted': records_inserted,
    }
    return render(request, 'upload_success.html', context)