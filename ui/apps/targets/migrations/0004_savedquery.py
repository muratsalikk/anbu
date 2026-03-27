from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("targets", "0003_targetaudit_snapshots"),
    ]

    operations = [
        migrations.CreateModel(
            name="SavedQuery",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(db_index=True, max_length=255, unique=True)),
                ("datasource", models.CharField(db_index=True, max_length=255)),
                ("created_by", models.CharField(db_index=True, max_length=255)),
                ("sql_text", models.TextField(blank=True, default="")),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "saved_query",
                "ordering": ["name"],
            },
        ),
    ]
