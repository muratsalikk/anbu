from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.db import migrations, models


def seed_property_files(apps, schema_editor):  # type: ignore[no-untyped-def]
    PropertyFile = apps.get_model("targets", "PropertyFile")
    root = Path(settings.BASE_DIR).parent
    seed_paths = [
        ("application.properties", root / "application.properties"),
        ("datasources.properties", root / "datasources.properties"),
        ("actions.properties", root / "actions.properties"),
        ("helper.properties", root / "helper.properties"),
    ]
    for name, path in seed_paths:
        if PropertyFile.objects.filter(name=name).exists():
            continue
        content = ""
        if path.exists() and path.is_file():
            content = path.read_text(encoding="utf-8")
        PropertyFile.objects.create(name=name, content=content)


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="PropertyFile",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "name",
                    models.CharField(
                        choices=[
                            ("application.properties", "application.properties"),
                            ("datasources.properties", "datasources.properties"),
                            ("actions.properties", "actions.properties"),
                            ("helper.properties", "helper.properties"),
                        ],
                        max_length=128,
                        unique=True,
                    ),
                ),
                ("content", models.TextField(blank=True, default="")),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "property_file",
                "ordering": ["name"],
            },
        ),
        migrations.CreateModel(
            name="RuleAudit",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("rule_name", models.CharField(db_index=True, max_length=255)),
                ("last_edited_at", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("last_edited_by", models.CharField(max_length=255)),
            ],
            options={
                "db_table": "rule_audit",
                "ordering": ["-last_edited_at", "-id"],
            },
        ),
        migrations.RunPython(seed_property_files, migrations.RunPython.noop),
    ]
