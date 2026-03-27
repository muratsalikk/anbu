from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("targets", "0004_savedquery"),
    ]

    operations = [
        migrations.AddField(
            model_name="targetaudit",
            name="change_notes",
            field=models.TextField(blank=True, default=""),
        ),
    ]
