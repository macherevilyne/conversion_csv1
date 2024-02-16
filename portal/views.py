# Importing modules
from django.utils import timezone
from django.shortcuts import render, redirect
from . db_commands import *
from . forms import UploadFileForm
from django.urls import reverse
import os
import mysql.connector
import subprocess
import configparser
from . models import *
from datetime import datetime
from django.db.models import Q
from collections import Counter
from sqlalchemy import create_engine

"""A method for displaying a form for uploading files, opening, saving and reading csv files only. 
Saving information about uploaded files in the admin panel. 
Returns a method for creating tables and reading data from files"""

def connect_use_db(request):
    print('---Старт connect_use_db', request.method)

    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_quarter_string = f'{current_year % 100}Q{current_quarter}'

    uploaded_files_names = []
    expected_files_all = UploadedFileChecklist.objects.filter(
        Q(file_name__contains=current_quarter_string) | Q(file_name='AdditionalData.csv')
    ).values_list('file_name', flat=True)

    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            uploaded_file = request.FILES['file']
            path = 'h:\IT\First_project'
            os.makedirs(path, exist_ok=True)
            file_path = os.path.join(path, uploaded_file.name)

            with open(file_path, 'wb') as destination:
                for chunk in uploaded_file.chunks():
                    destination.write(chunk)
            try:

                if not os.path.isdir(path):
                    raise FileNotFoundError(
                        f"Path '{path}' does not exist.Check that the path in the configuration file is correct")
            except FileNotFoundError as e:
                print(f"Error: {e}")
                exit()

            files = os.listdir(path)
            csv_files = [f for f in files if f.endswith('.csv')]

            file_upload = UploadedFile.objects.create(
                user=request.user,
                file=uploaded_file.name,
                uploaded_at=timezone.now()

            )
            file_upload.save()

            expected_files_without_additionaldata = [file for file in expected_files_all if file != 'AdditionalData.csv']
            uploaded_files = UploadedFile.objects.all()
            uploaded_files_names = [uploaded_file.file for uploaded_file in uploaded_files]
            is_four_files_uploaded = all(file in uploaded_files_names for file in expected_files_without_additionaldata)
            is_additionadata_uploaded = 'AdditionalData.csv' in uploaded_files_names
            is_lastuploaded_file_additionaldata = uploaded_file.name == 'AdditionalData.csv'

            desired_four_files = []
            for expected_file in expected_files_without_additionaldata:
                for csv_file in csv_files:
                    if expected_file in csv_file:
                        desired_four_files.append(csv_file)

            if desired_four_files:
                print("Найденные файлы:", desired_four_files)
            else:
                print("Файлы из коллекции 'expected_files' не найдены в списке csv_files.")

            if uploaded_file.name not in desired_four_files and uploaded_file.name != 'AdditionalData.csv':
                error_msg = f"Uploaded file {uploaded_file.name} is not in the expected files list."
                return render(request, 'upload_error.html', {'error_msg': error_msg})

            request.session['uploaded_files_names'] = uploaded_files_names
            request.session['is_four_files_uploaded'] = is_four_files_uploaded
            request.session['is_lastuploaded_file_additionaldata'] = is_lastuploaded_file_additionaldata
            request.session['desired_four_files'] = desired_four_files
            request.session['uploaded_file'] = uploaded_file.name
            request.session['path'] = path
            request.session['is_additionadata_uploaded'] = is_additionadata_uploaded

            return redirect('reading_files')

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


"""The method creates a connection with the Mysql server, if all four checklist files are uploaded via the form, 
a database is created or connected to it. Downloaded files are read, including if additionaldata is loaded. 
Returns the page in cases of successful loading, if not, then the page with an error.The database connection is being closed"""

def reading_files(request):
    records_inserted = 0
    config = configparser.ConfigParser()
    config.read('config_conversion_csv.ini')
    db_name = config['conversion_csv']['DATABASE']

    is_four_files_uploaded = request.session.get('is_four_files_uploaded')
    is_lastuploaded_file_additionaldata = request.session.get('is_lastuploaded_file_additionaldata')
    desired_four_files = request.session.get('desired_four_files')
    uploaded_file = request.session.get('uploaded_file', None)
    path = request.session.get('path')
    is_additionadata_uploaded = request.session.get('is_additionadata_uploaded')
    uploaded_files_names = request.session.get('uploaded_files_names')
    files_counter = Counter(uploaded_files_names)

    if is_four_files_uploaded:
        cnx = connection_to_db(config, db_name)
        cursor = cnx.cursor()

        try:
            records_inserted = 0
            reloading_files = None

            if is_lastuploaded_file_additionaldata == False:
                if files_counter.get(uploaded_file) > 1:
                    reloading_files = True
                else:
                    for file in desired_four_files:
                        table_name = file.split("_")[-1].split(".")[0].lower()
                        current_file_path = os.path.join(path, file)
                        create_table_four(cursor, cnx, table_name)
                        records_inserted += insert_four(current_file_path, cursor, table_name)
                        request.session['table_name'] = table_name
                        request.session['db_name'] = db_name

            if reloading_files == True:
                return render(request, 'reloading_the_file.html')

            if is_additionadata_uploaded:
                current_file_path = os.path.join(path, 'AdditionalData.csv')
                create_table_additionaldata(cursor, cnx)
                records_inserted += insert_additionaldata(current_file_path, cursor)

            disconnection_to_db(cnx, cursor)

        except mysql.connector.errors.ProgrammingError as error:
            error_msg = str(error)
            print(f"Error during SQL execution: {error_msg}")
            return render(request, 'upload_error.html', {'error_msg': error_msg})

        except mysql.connector.IntegrityError as error:
            error_msg = str(error)
            if error.errno == 1062:
                print(
                    f' Uploaded file: {uploaded_file} THERE ARE DUPLICATE RECORDS ON DOWNLOAD, NO DUPLICATES WILL BE ADDED')
                return render(request, 'upload_error.html', {'error_msg': error_msg})
            else:
                print(f"Last uploaded file: {uploaded_file}")
                print(f"Error: {error}")

        return redirect(
            reverse('upload_success') + f'?uploaded_file={uploaded_file}&records_inserted={records_inserted}')

    return redirect('connect_use_db')


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


"""The method checks whether the file has been downloaded again, if so, returns a page with a question to the user,
does he want to reload and overwrite the data? If the user clicks "Yes", a backup copy of the table in the database is created, 
time step, a new table with updated data is created.If not, the main page with the form is returned"""

def backup_table(request):
    records_inserted = 0
    path_backup_table = r'h:\IT\First_project\backup_table'
    os.makedirs(path_backup_table, exist_ok=True)

    uploaded_file = request.session.get('uploaded_file')
    table_name = request.session.get('table_name')
    db_name = request.session.get('db_name')
    path = request.session.get('path')
    current_file_path = os.path.join(path, uploaded_file)

    config = configparser.ConfigParser()
    config.read('config_conversion_csv.ini')
    cnx = connection_to_db(config, db_name)
    cursor = cnx.cursor()

    try:
        need_backup_table = False

        if check_records_updated(request):
            need_backup_table = True

        if need_backup_table == True:
            engine = create_engine(
                f"mysql+mysqlconnector://{config['conversion_csv']['USER']}:{config['conversion_csv']['PASSWORD']}@{config['conversion_csv']['HOST']}/{db_name}")
            df_original = pd.read_sql(f"SELECT * FROM {table_name}", engine)
            backup_table_name = f"backup_{table_name}_{datetime.now().strftime('%d.%m.%Y.%H-%M-%S')}"
            df_original.to_sql(backup_table_name, engine, index=False, if_exists='replace')
            print(f"Backup of {db_name}.{table_name} created successfully as {backup_table_name}")
            records_inserted += insert_four(current_file_path, cursor, table_name)
            disconnection_to_db(cnx, cursor)

    except mysql.connector.errors.ProgrammingError as error:
        error_msg = str(error)
        print(f"Error during SQL execution: {error_msg}")
        return render(request, 'upload_error.html', {'error_msg': error_msg})

    except mysql.connector.IntegrityError as error:
        error_msg = str(error)
        if error.errno == 1062:
            print(
                f' Uploaded file: {uploaded_file} THERE ARE DUPLICATE RECORDS ON DOWNLOAD, NO DUPLICATES WILL BE ADDED')
            return render(request, 'upload_error.html', {'error_msg': error_msg})
        else:
            print(f"Last uploaded file: {uploaded_file}")
            print(f"Error: {error}")

    except subprocess.CalledProcessError as e:
        print(f"Error creating backup: {e}")
        return render(request, 'upload_error.html', {'error_msg': f"Error creating backup: {e}"})

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return render(request, 'upload_error.html', {'error_msg': f"An unexpected error occurred: {e}"})

    return redirect(
        reverse('upload_success') + f'?uploaded_file={uploaded_file}&records_inserted={records_inserted}')


"""A method to check if the file is being downloaded again?"""

def check_records_updated(request):
    uploaded_file = request.session.get('uploaded_file')
    uploaded_files_names = request.session.get('uploaded_files_names')
    files_counter = Counter(uploaded_files_names)
    if files_counter.get(uploaded_file) > 1:
        return True
