from django import forms

import os



"""This is a file upload form.The clean_file() method checks that the uploaded file is a valid CSV file.
 If the uploaded file is not a CSV file or if the file has not been uploaded, the clean_file() method generates 
 a ValidationError exception with the corresponding error message that will be displayed to the user."""

class UploadFileForm(forms.Form):
    file = forms.FileField(label='File',widget=forms.ClearableFileInput(),
                        error_messages={'required': 'Please choose a file', 'invalid': 'File must be in CSV format'})

    def clean_file(self):
        file = self.cleaned_data.get('file')
        if file:
            filename = file.name
            ext = os.path.splitext(filename)[1]
            if ext.lower() != '.csv':
                raise forms.ValidationError("File must be in CSV format")
        else:
            raise forms.ValidationError("Please choose a file")
        return file
