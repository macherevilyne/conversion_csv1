# DashboardUI

A web application in which the user connects to the My SQL database using the parameters specified in the config_django_db.ini, 
config_conversion_csv.
The user logs in using 2FA authorization with a one-time password when using the Google auth application.
The user uploads the file through the file upload form, the file is read, and the data is transferred to the mysql database.
When duplicating unique fields, the data does not get into the database. Unique fields (Product,Policy_Number, Subproduct). 
For the correct operation of the web application, it is necessary:
1)Import requirements.txt
2)Mysql databases must be created in advance, for example, using the MySQL Workbench client, via the create database name request;
one database is used for user registration, and the second for file downloads 
In the config.ini file, you must specify the parameters of the Mysql database to be created: database,host, user, password.
3)In settings.py there is a variable MEDIA_ROOT = 'h:\IT\First_project ', here is the path to save the files. 
There is a PATH variable in portal\views, this is the path to read the saved file, change it if necessary. 
4)You need the Google Authenticator app on your phone or tablet to read the QR code
5)Before starting the project, you will need an IDE code editor. Write the cd conversion_csv command in the terminal, 
then you need to perform the migration using the python command manage.py makemigrations
and python manage.py migrate. To manage, create a superuser using the python command  python manage.py createsuperuser
6)You can run the project: python manage.py start the server. go to http://127.0.0.1:8000/admin , log in as a superuser. 
This is the admin panel where you can create users in the USERS section. In OTP_TOTP, in order to work correctly, 
it is necessary to assign a QR code to each created user of TOTP devices
for work. When you click "Downloaded Files", information about who uploaded which files and when will be displayed.
7)After you have created a user, you can go to http://127.0.0.1:8000 / and log in, upload files