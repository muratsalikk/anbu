from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("targets", "0006_anburesult_baseline_index"),
    ]

    operations = [
        migrations.DeleteModel(
            name="PropertyFile",
        ),
    ]
