from __future__ import annotations


class TargetsDbRouter:
    postgres_tables = {"anbu_result"}

    def db_for_read(self, model, **hints):  # type: ignore[no-untyped-def]
        if model._meta.db_table in self.postgres_tables:
            return "data_store"
        return None

    def db_for_write(self, model, **hints):  # type: ignore[no-untyped-def]
        if model._meta.db_table in self.postgres_tables:
            return "data_store"
        return None

    def allow_relation(self, obj1, obj2, **hints):  # type: ignore[no-untyped-def]
        if (
            obj1._meta.db_table in self.postgres_tables
            or obj2._meta.db_table in self.postgres_tables
        ):
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):  # type: ignore[no-untyped-def]
        if model_name in {"anburesult"}:
            return db == "data_store"
        if db == "data_store":
            return False
        return None
