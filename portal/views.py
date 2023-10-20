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
from .models import UploadedFile, UploadedFileChecklist
from datetime import datetime
from django.db.models import Q



"""A function that opens a connection to a MySQL database using the parameters specified in the Django settings. 
Creates a table if it does not exist. Next comes the file upload form, if the form is valid when uploading files and it does not exist in the path that is specified, 
saves the file. Next, CSV files are read from the specified directory and data is written to the MySQL database."""


def connect_use_db(request):



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







            # Reading all files in a directory with the csv extension
            files = os.listdir(path)

            csv_files = [f for f in files if f.endswith('.csv') ]




            # Read CSV files and write data to MySQL database






            last_uploaded_file = uploaded_file.name
            file_upload = UploadedFile.objects.create(
                user=request.user,
                file=uploaded_file.name,
                uploaded_at=timezone.now()

            )
            file_upload.save()



            # Interface for displaying the checklist in the template

            current_year = datetime.now().year
            current_quarter = (datetime.now().month - 1) // 3 + 1
            current_quarter_string = f'{current_year % 100}Q{current_quarter}'
            checklist = UploadedFile.objects.all()  # all uploaded files from the form via the interface


            uploaded_files = [uploaded_file.file for uploaded_file in
                              checklist]  # a list of file names that have already been uploaded.

            print(uploaded_files, 'Загруженные файлы')

            expected_files = UploadedFileChecklist.objects.filter(
                Q(file_name__contains=current_quarter_string) | Q(file_name='AdditionalData.csv')
            ).values_list('file_name', flat=True)  # файлы из чек листа в админке взависимости от года и квартала
            print(expected_files, 'фАЙЛЫ ИЗ ЧЕК ЛИСТА')

            expected_files_filtered = [file for file in expected_files if file != 'AdditionalData.csv']
            print(expected_files_filtered, 'Все кроме additionadata')

            all_files_uploaded = all(file in uploaded_files for file in expected_files_filtered)

            print(all_files_uploaded)

            # Reading connection parameters from the configuration file
            config = configparser.ConfigParser()
            config.read('config_conversion_csv.ini')

            # connect to server

            cnx = mysql.connector.connect(
                host=config['conversion_csv']['HOST'],
                user=config['conversion_csv']['USER'],
                password=config['conversion_csv']['PASSWORD'],

            )

            # Create cursor
            cursor = cnx.cursor()
            db_name = config['conversion_csv']['DATABASE']
            cursor.execute("SHOW DATABASES")
            databases = [database[0] for database in cursor]
            print(databases)
            table_name = None
            current_file_path = None
            additional_data_loaded = False

            if all_files_uploaded:
                print('Первый комент')

                # Checking the existence of the database, if the database does not exist, then creates a database and connects if it exists, then connects

                if db_name not in databases:
                    cursor.execute(f"CREATE DATABASE {db_name}")

                    print(f"Create database {db_name}")


                else:
                    print(f'The database {db_name} already exists')
                cursor.close()
                cnx.close()



                cnx = mysql.connector.connect(
                    host=config['conversion_csv']['HOST'],
                    user=config['conversion_csv']['USER'],
                    password=config['conversion_csv']['PASSWORD'],
                    database=db_name  # Add the database name here
                )
                cursor = cnx.cursor()
                print('Начало заполнения')

                print('Начало заполнения. Вся коллекция', csv_files)



                try:
                    print('Второй комент')
                    print(uploaded_file.name)
                    print(csv_files)


                    print('Третий комент')
                    for file in csv_files:

                        print('Четрвый комент')

                        table_name = file.split("_")[-1].split(".")[0].lower()
                        print(table_name)
                        print('Текущий файл', file)
                        current_file_path = os.path.join(path, file)
                        print(current_file_path)


                        if file == 'AdditionalData.csv' and not additional_data_loaded:
                            print(additional_data_loaded)
                            print('Создание таблицы Additionaldata')
                            cursor.execute(
                                            f"CREATE TABLE IF NOT EXISTS AdditionalData (ClosingDate VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "GainLossCarryForward  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "IntangibleAssets VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "MVAssets VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "MVBonds VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "MVMortgage VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "TransitoryAssets VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Zillmer VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "2312TotalRIShare VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "2322TotalRIShare VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "2332TotalRIShare VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "23TotalStatReserves VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "24TotalUL VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "196101RWBDE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "196201RWBAT VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "230601HRESolvencyCover VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "098003PBEUR VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "210301Frisby VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "081001Participations VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Risk_InitialPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Retail_InitialPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "NonRetailInitialPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Risk_RecurringPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Retail_RecurringPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "NonRetail_RecurringPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Marketing VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Underwriting VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Administration_systems  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "SalariesWages VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "SocialSecurityRenumeration VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "StaffCar VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "OtherPersonnelCosts VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "DirectorsFeesCharges VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "TravelExpenses VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "OfficeCost VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Depreciation VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "TelephoneCommunication VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "OtherOfficeCosts VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "BankCharges VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "ITExpenses VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "OtherOperatingExpenses VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Audit VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Lawyers VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "OtherProfessionals VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "DutiesManagementFees VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "DevelopmentBudget VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "AmortizationSetUpCosts VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Total VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Note_Home TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Note_S2Figures TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "Note_Portfolio TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
                                                "UNIQUE KEY unique_additionaldata (ClosingDate,GainLossCarryForward, IntangibleAssets))")

                            additional_data_loaded = True
                            print(additional_data_loaded)
                            cnx.commit()
                            print('Создалась таблица Additionaldata')




                        else:
                            print('Создание остальных TABLE NAME')
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

                            cnx.commit()



                            print('Создалась таблица TABLE NAME')

                        records_inserted = 0
                        additional_data_loaded = False
                        print(additional_data_loaded, 'незнаю зачем')
                        if file == 'AdditionalData.csv' and not additional_data_loaded:
                            print('Считывания Additionaldata')
                            print(additional_data_loaded)

                            df = pd.read_csv(current_file_path, low_memory=False, delimiter=';', encoding='cp1252')
                            df = df.where(pd.notnull(df), '')


                            for index, row in df.iterrows():
                                print('Добавления данных в таблицу Additionaldata')

                                sql = "INSERT INTO AdditionalData (ClosingDate, GainLossCarryForward, IntangibleAssets, MVAssets, MVBonds, MVMortgage, TransitoryAssets, Zillmer," \
                                                                                                              "2312TotalRIShare, 2322TotalRIShare, 2332TotalRIShare, 23TotalStatReserves, 24TotalUL, 196101RWBDE, 196201RWBAT, " \
                                                                                                              "230601HRESolvencyCover, 098003PBEUR, 210301Frisby, 081001Participations, Risk_InitialPPE, Retail_InitialPPE, NonRetailInitialPPE, Risk_RecurringPPE, Retail_RecurringPPE, " \
                                                                                                              "NonRetail_RecurringPPE,Marketing, Underwriting,Administration_systems , SalariesWages, SocialSecurityRenumeration, StaffCar, OtherPersonnelCosts, " \
                                                                                                              "DirectorsFeesCharges,TravelExpenses,OfficeCost,Depreciation,TelephoneCommunication,OtherOfficeCosts,BankCharges,ITExpenses," \
                                                                                                              "OtherOperatingExpenses, Audit,Lawyers,OtherProfessionals, DutiesManagementFees,DevelopmentBudget,AmortizationSetUpCosts," \
                                                                                                              "Total,Note_Home,Note_S2Figures,Note_Portfolio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                                                                                              "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                val = tuple(row)

                                cursor.execute(sql, val)
                                records_inserted += cursor.rowcount

                            additional_data_loaded = True
                            print(additional_data_loaded)
                            print('Данные добавились')


                            print(f"TOTAL RECORDS MADE TO DATABASE: {records_inserted}")
                            print(f"TOTAL RECORDS TO BE MADE FROM DATAFRAME TO DATABASE:", len(df))



                        else:
                            print('Считывания остальных файлов')

                            df = pd.read_csv(current_file_path, low_memory=False)
                            df = df.where(pd.notnull(df), '')
                            print(df)
                            for index, row in df.iterrows():


                                sql = f"INSERT INTO {table_name} (Product_Group, Product, Subproduct, Policy_Number, Status, Gender, Age, Joint_Life," \
                                                                                  " Start_Date, Lapse_Date, End_Date, Annual_Premium, Single_Premium, Premium_Indexation, Changing_Premium, " \
                                                                                  "Fonds_Value, PVPrem, PVFP, PVCost, PVSCR, Statutory_Res, SII_Res, Death_Benefit_Sum, Sum_of_Salary, " \
                                                                                  "Disability_Annuity, Benefit_Indexation, Waiting_Period, Coverage_Style, Benefit_Duration, NBM, CoC, " \
                                                                                  "Combined_Ratio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                                                                  "%s,%s,%s,%s,%s,%s)"


                                val = tuple(row)

                                cursor.execute(sql, val)
                                records_inserted += cursor.rowcount



                            print('Вставка остальных таблиц')

                            print(f"TOTAL RECORDS MADE TO DATABASE: {records_inserted}")
                            print(f"TOTAL RECORDS TO BE MADE FROM DATAFRAME TO DATABASE:", len(df))



                    cnx.commit()
                    cursor.close()
                    cnx.close()
                    return redirect(
                                            reverse('upload_success') + f'?uploaded_file={uploaded_file}&records_inserted=')


                except mysql.connector.errors.ProgrammingError as error:
                    print('Срабатывает исключение 1')
                    error_msg = str(error)
                    cursor.execute(f"USE {db_name}")
                    print('Срабатывает исключение 2')
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    print(f"Error during SQL execution: {error_msg}")
                    print('Срабатывает исключение 3')
                    os.remove(current_file_path)
                    return render(request, 'upload_error.html', {'error_msg': error_msg})


                except mysql.connector.IntegrityError as error:
                    error_msg = str(error)
                    print('Срабатывает исключение 4')
                    if error.errno == 1062:  # код ошибки дубликата ключа
                        print('Срабатывает исключение 5')
                        print(
                                        f' Uploaded file: {last_uploaded_file} THERE ARE DUPLICATE RECORDS ON DOWNLOAD, NO DUPLICATES WILL BE ADDED')

                        return render(request, 'upload_error.html', {'error_msg': error_msg})
                    else:
                        print('Срабатывает исключение 6')
                        print(f"Last uploaded file: {last_uploaded_file}")
                        print(f"Error: {error}")


    else:
        form = UploadFileForm()

    # Interface for displaying the checklist in the template

    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_quarter_string = f'{current_year % 100}Q{current_quarter}'
    all_files = UploadedFileChecklist.objects.filter(
        Q(file_name__contains=current_quarter_string) | Q(file_name='additionaldata.csv')
    )  # all files from the checklist created in the admin panel

    checklist = UploadedFile.objects.all()  # all uploaded files from the form via the interface

    uploaded_files = [uploaded_file.file for uploaded_file in
                      checklist]  # a list of file names that have already been uploaded.







    context = {

        'form': form,
        'checklist':checklist,
        'uploaded_files':uploaded_files,
        'all_files': all_files,

    }
    return render(request, 'upload_file.html', context)



def generate_db_for_4_files():
    print('4')
def generate_db_for_1_file():
    print('1')

def connection_to_db():
    print('connect to db')

def descconnection_to_db():
    print('descconnection_db')




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