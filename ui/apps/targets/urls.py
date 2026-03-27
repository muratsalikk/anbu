from django.urls import path

from . import views


app_name = "targets"

urlpatterns = [
    path("app-logo", views.app_logo, name="app_logo"),
    path("settings/", views.anbu_settings, name="settings"),
    path("settings/datasources/", views.datasource_list, name="datasource_list"),
    path("settings/datasources/new/", views.datasource_edit, name="datasource_new"),
    path(
        "settings/datasources/<str:source_name>/edit/",
        views.datasource_edit,
        name="datasource_edit",
    ),
    path("settings/actions/", views.action_list, name="action_list"),
    path("settings/actions/new/", views.action_edit, name="action_new"),
    path(
        "settings/actions/<str:action_name>/edit/",
        views.action_edit,
        name="action_edit",
    ),
    path("targets/", views.targets_list, name="list"),
    path("targets/new/", views.target_edit, name="new"),
    path("targets/new/with-ai/", views.target_new_with_ai, name="new_with_ai"),
    path("targets/<str:target_name>/", views.target_detail, name="detail"),
    path("targets/<str:target_name>/edit/", views.target_edit, name="edit"),
    path(
        "targets/<str:target_name>/audit/<int:audit_id>/view/",
        views.target_audit_view,
        name="audit_view",
    ),
    path(
        "targets/<str:target_name>/audit/<int:audit_id>/changes/",
        views.target_audit_changes,
        name="audit_changes",
    ),
    path(
        "targets/<str:target_name>/audit/<int:audit_id>/restore/",
        views.target_audit_restore,
        name="audit_restore",
    ),
    path(
        "targets/<str:target_name>/history/instances/",
        views.target_history_instances,
        name="history_instances",
    ),
    path(
        "targets/<str:target_name>/history/run-normal-actions/",
        views.target_history_run_normal_actions,
        name="history_run_normal_actions",
    ),
    path(
        "targets/<str:target_name>/history/delete/",
        views.target_history_delete,
        name="history_delete",
    ),
    path(
        "targets/<str:target_name>/history/import/",
        views.target_history_import,
        name="history_import",
    ),
    path("explore/", views.explore, name="explore"),
    path("logs/", views.logs_page, name="logs"),
    path("test-actions/", views.test_actions, name="test_actions"),
    path("help/", views.help_page, name="help"),
    path("help/<str:topic>/", views.help_topic_page, name="help_topic"),
]
