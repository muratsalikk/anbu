from django.db import models


class AnbuResult(models.Model):
    id = models.BigAutoField(primary_key=True)
    evaluated_at = models.DateTimeField()
    target_name = models.TextField()
    metric_name = models.TextField(blank=True, null=True)
    metric_value = models.BigIntegerField(blank=True, null=True)
    severity = models.SmallIntegerField(blank=True, null=True)
    state = models.TextField()
    critical_val = models.BigIntegerField(blank=True, null=True)
    major_val = models.BigIntegerField(blank=True, null=True)
    minor_val = models.BigIntegerField(blank=True, null=True)
    message = models.TextField(blank=True, null=True)
    action_name = models.TextField(blank=True, null=True)
    datasource = models.TextField(blank=True, null=True)
    scheduler_name = models.TextField(blank=True, null=True)
    tags = models.TextField(blank=True, null=True)
    baseline = models.BigIntegerField(blank=True, null=True)
    deviation = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "anbu_result"


class TargetAudit(models.Model):
    id = models.BigAutoField(primary_key=True)
    target_name = models.CharField(max_length=255, db_index=True)
    edited_at = models.DateTimeField(auto_now_add=True, db_index=True)
    edited_by = models.CharField(max_length=255)
    change_notes = models.TextField(blank=True, default="")
    env_content = models.TextField(blank=True, default="")
    sql_content = models.TextField(blank=True, default="")
    hql_content = models.TextField(blank=True, default="")

    class Meta:
        db_table = "target_audit"
        ordering = ["-edited_at", "-id"]


class PropertyFile(models.Model):
    APPLICATION = "application.properties"
    DATASOURCES = "datasources.properties"
    ACTIONS = "actions.properties"
    HELPER = "helper.properties"

    NAME_CHOICES = [
        (APPLICATION, APPLICATION),
        (DATASOURCES, DATASOURCES),
        (ACTIONS, ACTIONS),
        (HELPER, HELPER),
    ]

    name = models.CharField(max_length=128, unique=True, choices=NAME_CHOICES)
    content = models.TextField(blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "property_file"
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class SavedQuery(models.Model):
    name = models.CharField(max_length=255, unique=True, db_index=True)
    datasource = models.CharField(max_length=255, db_index=True)
    created_by = models.CharField(max_length=255, db_index=True)
    sql_text = models.TextField(blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "saved_query"
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name
