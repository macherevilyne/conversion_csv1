from django.contrib import admin
from .models import UploadedFile, UploadedFileChecklist
from django.urls import reverse
from django.http import HttpResponseRedirect


# Register model
admin.site.register(UploadedFile)



class UploadedFileChecklistAdmin(admin.ModelAdmin):
    list_display = ('file_name',)  # Отображаем поля в списке объектов

    def copy_selected_files(self, request, queryset):
        for obj in queryset:
            new_obj = obj
            new_obj.id = None
            new_obj.save()
            return HttpResponseRedirect(reverse('admin:portal_uploadedfilechecklist_change', args=[new_obj.id]))
    copy_selected_files.short_description = "Copy and edit selected files"  # Название действия

    actions = [copy_selected_files]  # Добавляем действие к списку действий

# Регистрируем административную конфигурацию для модели UploadedFileChecklist
admin.site.register(UploadedFileChecklist, UploadedFileChecklistAdmin)