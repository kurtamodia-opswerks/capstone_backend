from django.urls import path, include
from rest_framework.routers import DefaultRouter
from core.views import DatasetViewSet

router = DefaultRouter()
router.register(r'dataset', DatasetViewSet, basename='dataset')

urlpatterns = [
    path('', include(router.urls)),
]
