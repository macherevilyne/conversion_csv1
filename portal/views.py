# Importing modules
from django.utils import timezone
from django.shortcuts import render, redirect
from django.core.files.storage import FileSystemStorage

from .db_commands import *
from .forms import UploadFileForm
from django.urls import reverse
import os

import mysql.connector
import configparser
from .models import UploadedFile, UploadedFileChecklist
from datetime import datetime
from django.db.models import Q



"""A function that opens a connection to a MySQL database using the parameters specified in the Django settings. 
Creates a table if it does not exist. Next comes the file upload form, if the form is valid when uploading files and it does not exist in the path that is specified, 
saves the file. Next, CSV files are read from the specified directory and data is written to the MySQL database."""


def connect_use_db(request):
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
            config = configparser.ConfigParser()
            config.read('config_conversion_csv.ini')
            db_name = config['conversion_csv']['DATABASE']


            table_name = None
            current_file_path = None
            additional_data_loaded = False

            if all_files_uploaded:
                print('Первый комент')

                # Checking the existence of the database, if the database does not exist, then creates a database and connects if it exists, then connects

                cnx = connection_to_db(config, db_name)
                cursor = cnx.cursor()

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
                            create_table_additionaldata(cursor, cnx)
                        else:
                            create_table_four(cursor, cnx, table_name)


                        records_inserted = 0
                        additional_data_loaded = False
                        print(additional_data_loaded, 'незнаю зачем')

                        if file == 'AdditionalData.csv' and not additional_data_loaded:
                            records_inserted += insert_additionaldata(current_file_path, cursor)
                        else:
                            records_inserted += insert_four(current_file_path, cursor, table_name)

                    descconnection_to_db(cnx,cursor)

                    return redirect(reverse('upload_success') + f'?uploaded_file={uploaded_file}&records_inserted=')


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