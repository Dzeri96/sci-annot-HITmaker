"""web URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path

from .views import Assignment, review, review_page, index, compare_predictions

urlpatterns = [
    path('', index, name='index'),
    path('admin/', admin.site.urls),
    path('assignment/<str:page_id>/<str:assignment_id>', Assignment.as_view(), name='assignment'),
    path('review/', review, name='review'),
    path('review/<str:page_id>', review_page, name='review_page'),
    path('predictions/<str:page_id>', compare_predictions, name='compare_predictions')
]
