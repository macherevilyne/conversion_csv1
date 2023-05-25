from django.urls import path
from .import views
from django.contrib.auth import views as auth_views

# Paths to pages
urlpatterns = [
    path('login/', views.login_view, name='login'),
    path('logout/', auth_views.LogoutView.as_view(next_page='/'), name='logout'),
    path('token/', views.token_view, name='token'),
    path('no2fa/', views.no2fa, name='no2fa'),

]