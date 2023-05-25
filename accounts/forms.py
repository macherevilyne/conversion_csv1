from django import forms


# Form for login
class LoginForm(forms.Form):
    username = forms.CharField(max_length=255)
    password = forms.CharField(widget=forms.PasswordInput)

# Form for token
class TokenForm(forms.Form):
    token = forms.CharField(max_length=6)