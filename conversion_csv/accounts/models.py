from django.contrib.auth.models import AbstractUser

# Model USER and Hashing the password to all users except the superuser
class User(AbstractUser):


    def save(self, *args, **kwargs):

        if not self.is_superuser:
            self.set_password(self.password)
        super(User, self).save(*args, **kwargs)


