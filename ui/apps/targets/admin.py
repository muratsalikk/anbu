from django.contrib import admin
from django.forms import Textarea
from django.db import models

from .models import SavedQuery, TargetAudit

admin.site.site_header = "ANBU Alarm Management"
admin.site.site_title = "ANBU Admin"
admin.site.index_title = "User Management"


@admin.register(TargetAudit)
class TargetAuditAdmin(admin.ModelAdmin):
    list_display = ("target_name", "edited_by", "edited_at")
    search_fields = ("target_name", "edited_by")
    readonly_fields = ("edited_at",)
    ordering = ("-edited_at", "-id")


@admin.register(SavedQuery)
class SavedQueryAdmin(admin.ModelAdmin):
    list_display = ("name", "datasource", "created_by", "updated_at", "created_at")
    search_fields = ("name", "datasource", "created_by")
    readonly_fields = ("created_at", "updated_at")
    ordering = ("name",)
    formfield_overrides = {
        models.TextField: {"widget": Textarea(attrs={"rows": 20, "cols": 140})}
    }
