
from django.urls import path
from .import views

urlpatterns = [
    path('', views.connect_use_db, name='connect_use_db'),
    path('upload/reading_files/',views.reading_files, name='reading_files'),
    path('upload/backup_table/', views.backup_table, name='backup_table'),
    path('upload/success/', views.upload_success, name='upload_success'),
    path('upload/error/', views.upload_error, name='upload_error'),


]
