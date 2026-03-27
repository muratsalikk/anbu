from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("targets", "0002_anburesult"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="RuleAudit",
            new_name="TargetAudit",
        ),
        migrations.AlterModelTable(
            name="targetaudit",
            table="target_audit",
        ),
        migrations.RenameField(
            model_name="targetaudit",
            old_name="rule_name",
            new_name="target_name",
        ),
        migrations.RenameField(
            model_name="targetaudit",
            old_name="last_edited_at",
            new_name="edited_at",
        ),
        migrations.RenameField(
            model_name="targetaudit",
            old_name="last_edited_by",
            new_name="edited_by",
        ),
        migrations.AddField(
            model_name="targetaudit",
            name="env_content",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="targetaudit",
            name="sql_content",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="targetaudit",
            name="hql_content",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AlterModelOptions(
            name="targetaudit",
            options={"ordering": ["-edited_at", "-id"]},
        ),
    ]
