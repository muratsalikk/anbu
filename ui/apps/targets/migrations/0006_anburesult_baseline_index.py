from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("targets", "0005_targetaudit_change_notes"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS anbu_result_baseline_idx "
                "ON anbu_result (target_name, metric_name, evaluated_at DESC) "
                "INCLUDE (metric_value) "
                "WHERE metric_value IS NOT NULL;"
            ),
            reverse_sql=(
                "DROP INDEX CONCURRENTLY IF EXISTS anbu_result_baseline_idx;"
            ),
        ),
    ]
