from __future__ import annotations

import json
from django import forms


SQL_MODE_CHOICES = [
    ("single", "Single row"),
    ("multiline", "Multiline"),
]

DATASOURCE_TYPE_CHOICES = [
    ("POSTGRES", "POSTGRES"),
    ("ORACLE", "ORACLE"),
    ("MYSQL", "MYSQL"),
    ("MARIADB", "MARIADB"),
    ("SQLITE", "SQLITE"),
    ("CSV", "CSV"),
]


class TargetRuleForm(forms.Form):
    target_name = forms.CharField(max_length=128, label="Target Name")
    description = forms.CharField(widget=forms.Textarea(attrs={"rows": 2}))
    document_url = forms.CharField(required=False)
    dashboard_url = forms.CharField(required=False)
    tag_list = forms.CharField(required=False, help_text="Comma-separated tags")
    is_active = forms.BooleanField(required=False, initial=True)
    is_muted = forms.BooleanField(required=False, initial=False)
    data_source = forms.ChoiceField(choices=[])
    sql_timeout_sec = forms.IntegerField(min_value=0, initial=60)
    sql_jitter_sec = forms.IntegerField(min_value=0, initial=0)
    schedule_cron = forms.CharField(required=False)
    sql_mode = forms.ChoiceField(choices=SQL_MODE_CHOICES, initial="single")
    mute_between_enabled = forms.BooleanField(required=False)
    mute_between_rules = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"rows": 2}),
        help_text='JSON array, e.g. [{"start":"23:00","end":"06:00","days":["MON","TUE"]}]',
    )
    mute_until_enabled = forms.BooleanField(required=False)
    mute_until = forms.CharField(
        required=False,
        help_text="Date-time: YYYY-MM-DD HH:MM or ISO format",
    )
    metrics_json = forms.CharField(
        widget=forms.Textarea(attrs={"rows": 12}),
        help_text="Metrics JSON list. Keep CRITICAL/MAJOR/MINOR arrays.",
    )
    sql_text = forms.CharField(widget=forms.HiddenInput())
    original_name = forms.CharField(required=False, widget=forms.HiddenInput())
    original_query_file = forms.CharField(required=False, widget=forms.HiddenInput())

    for idx in range(1, 11):
        locals()[f"map_val{idx}"] = forms.CharField(required=False, label=f"MAP_VAL{idx}")
    del idx

    def __init__(self, *args, datasource_choices: list[str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        source_choices = datasource_choices or []
        self.fields["data_source"].choices = [(item, item) for item in source_choices]
        for name, field in self.fields.items():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs["class"] = "form-check-input"
                continue
            css = "form-control"
            if isinstance(field.widget, forms.Select):
                css = "form-select"
            field.widget.attrs["class"] = css
            if name == "target_name":
                field.widget.attrs["placeholder"] = "EXAMPLE.TARGET"

    def clean_target_name(self) -> str:
        return self.cleaned_data["target_name"].strip().upper()

    def clean_metrics_json(self) -> str:
        text = self.cleaned_data.get("metrics_json", "").strip()
        if not text:
            raise forms.ValidationError("metrics_json is required.")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise forms.ValidationError(f"metrics_json is not valid JSON: {exc}") from exc
        if not isinstance(parsed, list):
            raise forms.ValidationError("metrics_json must be a JSON list.")
        self.cleaned_data["metrics_parsed"] = parsed
        return text

    def clean(self):
        cleaned_data = super().clean()
        mapping: dict[str, str] = {}
        for idx in range(1, 11):
            key = f"map_val{idx}"
            alias = str(cleaned_data.get(key, "") or "").strip().upper()
            mapping[f"VAL{idx}"] = alias
            cleaned_data[key] = alias
        cleaned_data["mapping"] = mapping
        cleaned_data["metrics"] = cleaned_data.get("metrics_parsed", [])
        cleaned_data["sql_text"] = str(cleaned_data.get("sql_text", "") or "")
        cleaned_data["mute_between_rules"] = str(
            cleaned_data.get("mute_between_rules", "") or ""
        ).strip()
        cleaned_data["mute_until"] = str(cleaned_data.get("mute_until", "") or "").strip()
        return cleaned_data


class ExploreForm(forms.Form):
    datasource = forms.CharField(required=False)
    saved_query = forms.CharField(required=False)
    query_name = forms.CharField(required=False, widget=forms.HiddenInput())
    sql_text = forms.CharField(widget=forms.HiddenInput())

    def __init__(
        self,
        *args,
        datasource_choices: list[str] | None = None,
        saved_query_choices: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        ds = datasource_choices or []
        saved = saved_query_choices or []
        self.fields["datasource"].widget = forms.Select(choices=[(item, item) for item in ds])
        self.fields["saved_query"].widget = forms.Select(choices=[(item, item) for item in saved])
        for name, field in self.fields.items():
            if isinstance(field.widget, forms.HiddenInput):
                continue
            if isinstance(field.widget, forms.Select):
                field.widget.attrs["class"] = "form-select"
            else:
                field.widget.attrs["class"] = "form-control"

    def clean_query_name(self) -> str:
        return self.cleaned_data.get("query_name", "").strip().upper()


class ApplicationPropertiesForm(forms.Form):
    RULES_DIR = forms.CharField(required=False)
    DS_HOST = forms.CharField(required=False)
    DS_PORT = forms.CharField(required=False)
    DS_DBNAME = forms.CharField(required=False)
    DS_USER = forms.CharField(required=False)
    DS_PASS = forms.CharField(required=False)
    HELPER_TEXT_FILE = forms.CharField(required=False)
    SAVED_QUERIES_DIR = forms.CharField(required=False)
    ENGINE_LOG_FILE = forms.CharField(required=False)
    UI_LOG_FILE = forms.CharField(required=False)
    BACKUP_SCRIPT = forms.CharField(required=False)
    APP_LOGO_FILE = forms.CharField(required=False)
    AI_PROMPT_FILE = forms.CharField(required=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs["class"] = "form-control"


class DataSourceForm(forms.Form):
    datasource_name = forms.CharField(max_length=255, label="Data Source Name")
    datasource_type = forms.ChoiceField(
        choices=DATASOURCE_TYPE_CHOICES,
        label="TYPE",
        initial="POSTGRES",
    )
    datasource_user = forms.CharField(max_length=255, label="USER")
    datasource_password = forms.CharField(max_length=255, label="PASSWORD", required=False)
    datasource_dsn = forms.CharField(max_length=1024, label="DSN")
    original_name = forms.CharField(required=False, widget=forms.HiddenInput())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            css = "form-control"
            if isinstance(field.widget, forms.Select):
                css = "form-select"
            field.widget.attrs["class"] = css

    def clean_datasource_name(self) -> str:
        return str(self.cleaned_data.get("datasource_name", "")).strip().upper()

    def clean_datasource_type(self) -> str:
        return str(self.cleaned_data.get("datasource_type", "")).strip().upper()

    def clean_datasource_user(self) -> str:
        return str(self.cleaned_data.get("datasource_user", "")).strip()

    def clean_datasource_password(self) -> str:
        return str(self.cleaned_data.get("datasource_password", "")).strip()

    def clean_datasource_dsn(self) -> str:
        return str(self.cleaned_data.get("datasource_dsn", "")).strip()


class ActionPropertyForm(forms.Form):
    action_name = forms.CharField(max_length=255, label="Action Name")
    action_file_path = forms.CharField(max_length=1024, label="Action File Path")
    original_name = forms.CharField(required=False, widget=forms.HiddenInput())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            css = "form-control"
            field.widget.attrs["class"] = css

    def clean_action_name(self) -> str:
        return str(self.cleaned_data.get("action_name", "")).strip().upper()

    def clean_action_file_path(self) -> str:
        return str(self.cleaned_data.get("action_file_path", "")).strip()
