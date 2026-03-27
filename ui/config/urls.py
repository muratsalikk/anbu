from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import include, path
from django.views.generic import RedirectView

from apps.targets.views import AppLoginView, app_logout


urlpatterns = [
    path("admin/", admin.site.urls),
    path("accounts/login/", AppLoginView.as_view(), name="login"),
    path("accounts/logout/", app_logout, name="logout"),
    path("", RedirectView.as_view(pattern_name="targets:list", permanent=False)),
    path("", include("apps.targets.urls")),
]

# Local static serving fallback for environments without nginx/whitenoise.
urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
