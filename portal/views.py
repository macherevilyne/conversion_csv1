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
    print('---Старт connect_use_db', request.method)

    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_quarter_string = f'{current_year % 100}Q{current_quarter}'

    uploaded_files_names = []
    expected_files_all = UploadedFileChecklist.objects.filter(
        Q(file_name__contains=current_quarter_string) | Q(file_name='AdditionalData.csv')
    ).values_list('file_name', flat=True)
    print('expected_files_all ', expected_files_all)

    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():

            uploaded_file = request.FILES['file']
            fs = FileSystemStorage()
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


            files = os.listdir(path)
            csv_files = [f for f in files if f.endswith('.csv') ]
            print(csv_files, 'csv_files')



            file_upload = UploadedFile.objects.create(
                user=request.user,
                file=uploaded_file.name,
                uploaded_at=timezone.now()

            )
            file_upload.save()


            uploaded_files = UploadedFile.objects.all()
            print(uploaded_files)

            uploaded_files_names = [uploaded_file.file for uploaded_file in uploaded_files]

            print(uploaded_files_names, '--------------uploaded_files_names')

            expected_files_without_additionaldata = [file for file in expected_files_all if file != 'AdditionalData.csv']
            print(expected_files_without_additionaldata, 'Все кроме additionadata')



            is_four_files_uploaded = all(file in uploaded_files_names for file in expected_files_without_additionaldata)
            is_additionadata_uploaded = 'AdditionalData.csv' in uploaded_files_names
            is_lastuploaded_file_additionaldata = uploaded_file.name == 'AdditionalData.csv'
            print('State:'
                'is_four_files_uploaded',is_four_files_uploaded,
                  'is_additionadata_uploaded',is_additionadata_uploaded,
                  'is_lastuploaded_file_additionaldata', is_lastuploaded_file_additionaldata)


            print(is_four_files_uploaded)
            config = configparser.ConfigParser()
            config.read('config_conversion_csv.ini')
            db_name = config['conversion_csv']['DATABASE']


            table_name = None
            current_file_path = None


            if is_four_files_uploaded:
                print('Все четыре файла загружены')
                desired_four_files = []
                for expected_file in expected_files_without_additionaldata:
                    for csv_file in csv_files:
                        if expected_file in csv_file:
                            desired_four_files.append(csv_file)

                if desired_four_files:
                    print("Найденные файлы:", desired_four_files)
                else:
                    print("Файлы из коллекции 'expected_files' не найдены в списке csv_files.")



                cnx = connection_to_db(config, db_name)
                cursor = cnx.cursor()

                print('Начало заполнения. Вся коллекция', csv_files)

                try:

                    records_inserted = 0

                    if is_lastuploaded_file_additionaldata == False:
                        print('Начинаем заполнять is_lastuploaded_file_additionaldata')
                        for file in desired_four_files:

                            print('desured_four_files', desired_four_files)
                            table_name = file.split("_")[-1].split(".")[0].lower()
                            print('Текущая таблица',table_name, 'Текущий файл', file)
                            current_file_path = os.path.join(path, file)
                            create_table_four(cursor, cnx, table_name)

                            records_inserted += insert_four(current_file_path, cursor, table_name)

                    if is_additionadata_uploaded:
                        print('Начинаем заполнять is_additionadata_uploaded')

                        current_file_path = os.path.join(path, 'AdditionalData.csv')
                        print('Путь current_file_path', current_file_path)
                        create_table_additionaldata(cursor, cnx)
                        records_inserted += insert_additionaldata(current_file_path, cursor)


                    discconnection_to_db(cnx, cursor)




                    return redirect(
                        reverse('upload_success') + f'?uploaded_file={uploaded_file}&records_inserted=')





                except mysql.connector.errors.ProgrammingError as error:
                    print('Срабатывает исключение 1')
                    error_msg = str(error)
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
                            f' Uploaded file: {uploaded_file.name} THERE ARE DUPLICATE RECORDS ON DOWNLOAD, NO DUPLICATES WILL BE ADDED')

                        return render(request, 'upload_error.html', {'error_msg': error_msg})
                    else:
                        print('Срабатывает исключение 6')
                        print(f"Last uploaded file: {uploaded_file.name}")
                        print(f"Error: {error}")



    else:
        form = UploadFileForm()
        uploaded_files = UploadedFile.objects.all()
        uploaded_files_names = [uploaded_file.file for uploaded_file in uploaded_files]



    context = {
        'form': form,
        'uploaded_files_names':uploaded_files_names,
        'expected_files_all': expected_files_all,

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


