from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django_otp import devices_for_user
from django_otp.plugins.otp_totp.models import TOTPDevice

from .utils import generate_qr_code
from .forms import LoginForm, TokenForm


"""This function processes GET and POST requests to the login page and displays the template. 
Checking the validity of the form created by the user, if everything is successful, 
go to the page for authorization by qr code"""


def login_view(request):
    if request.method == 'POST':
        form = LoginForm(request.POST)
        if form.is_valid():
            username = form.cleaned_data['username']
            password = form.cleaned_data['password']
            # checking the created user
            user = authenticate(request, username=username, password=password)
            # creating a device for the user for authorization by qr code
            if user is not None:
                # list of devices for the user
                devices = devices_for_user(user)
                device_list = list(devices)
                # If the user already has a device, then the first device from the list is selected, its id is set to the session and a QR code is generated
                if len(device_list) > 0:
                    device = device_list[0]

                    request.session['device_id'] = device.id
                    qr_code = generate_qr_code(device.config_url)
                    context = {
                        'qr_code': qr_code,
                        'token_form': TokenForm,
                    }

                    return render(request, '2fa.html', context)
                else:

                    return redirect('no2fa')
            else:

                form.add_error(None, 'Invalid username or password')
        else:
            form = LoginForm()
    else:
        form = LoginForm()
    return render(request, 'login.html', {'form': form})


def no2fa(request):
    return render(request, 'no2fa.html')


"""This function processes the request to verify the entered authentication token (OTP) in the form provided by the user 
and verifies it using the TOTPDevice object. If the token passes verification, the user is considered authenticated and logged in, 
otherwise an error message is displayed and the user is prompted to try again. 
As in login_view(), a page with a two-factor authentication form is returned."""



def token_view(request):
    if request.method == 'POST':
        token_form = TokenForm(request.POST)
        if token_form.is_valid():
            device_id = request.session.get('device_id')
            if device_id:
                device = TOTPDevice.objects.get(id=device_id)
                if device.verify_token(token_form.cleaned_data['token']):
                    user = device.user
                    login(request, user)
                    return redirect('/')
                else:
                    token_form.add_error('token', 'Invalid token')
            else:
                return redirect('/')
    else:
        token_form = TokenForm()
    return render(request, '2fa.html', {'token_form': token_form})


""" Logout of authorization"""

def logout_view(request):
    logout(request)
    return redirect('/')

