from django.db import models

from accounts.models import User

"""This is a model for the admin panel so that the administrator can see by whom and when which file was uploaded"""


class UploadedFile(models.Model):

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    file = models.CharField(max_length=255)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.file} uploaded by {self.user} at {self.uploaded_at}"

"""This is a model for the admin panel so that the administrator can create a checklist"""
class UploadedFileChecklist(models.Model):
    file_name = models.CharField(max_length=255)
    def __str__(self):
        return self.file_name