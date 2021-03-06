"""sam URL Configuration

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
#from django.contrib import admin
from django.urls import path ,include
from .views import *

urlpatterns = [
   # path('admin/', admin.site.urls),
   #  path('monthly/<str:uid>/<str:pid>/<str:days>',sample_monthly),
   #  path('resample/<str:uid>/<str:pid>/<str:days>',sample_monthly),
   #  path('quaterly/<str:uid>/<str:pid>/<str:days>',sample_quaterly),
   #  path('weekly/<str:uid>/<str:pid>/<str:days>',sample_weekly),
   #  path('daily/<str:uid>/<str:pid>/<str:days>',daily),
    path('getdata/<str:uid>/<str:pid>/<str:name>',get_data)
]
