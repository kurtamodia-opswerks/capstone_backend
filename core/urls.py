from django.urls import path
from .views import DatasetUploadView

urlpatterns = [
    path("datasets/upload/", DatasetUploadView.as_view(), name="dataset-upload"),
]
