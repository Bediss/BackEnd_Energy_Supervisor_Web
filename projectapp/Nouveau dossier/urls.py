from django.urls import path, include

from . import views
from . import tasks
from . import views as jwt_views

from projectapp.apis import getTl
from projectapp.apis import cloneV3,cloneV4,cloneV5
from projectapp.apis import reports
from projectapp.apis import getSVG
from projectapp.apis import test
from projectapp.apis import cloneSynoptic

from .views import aa

from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)
from rest_framework.routers import DefaultRouter

# router = DefaultRouter()
# router.register(r'product', aa , basename='Product')

urlpatterns = [
    path('token/', TokenObtainPairView, name='token_obtain_pair'),
    path('token/refresh/', TokenRefreshView, name='token_refresh'),
    path('a/', aa, name='token_refresh'),
    # path('', include(router.urls)),
    path('', views.index, name='index'),

  
    # success
    path('display/', views._filter),
    path('filter/', views._filter),
    path('updatedelete/', views.updatedelete),
   #success
 
    path('sendid/', views.sendnewid),

    path('insertiot/', views.insertiot),
    path('getobjective/', views.getobjectivetest),


    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    path('cloneSynopticV2/', cloneSynoptic.cloneSynopticV2),
    path('cloneV5/', cloneV5.cloneReport_api),
    path('generatePDF/', views.generatePDF),
    path('getReports/', reports.getReports),
    path('getReportById/', reports.getReportById),
    path('getReportByName/', reports.getReportByName),
    path('getCounters/', views.getCounters),
    path('getMesures/', views.getMesures),
    path('getMLByEnergy/', views.getMLByEnergy),
    path('filterPrep/', views.filterPrep),
    path('getTl/', getTl.getTl),
    path('getTlSql/', getTl.getTlSql),
    path('getSVG/', getSVG.getSVG),
    #############Service Cluster################
    path('cluster/', views.cluster),
        #############Service Iot Inner################
    path('iotinner/', views.iotinner),
    path('test/', test.testRequest),
    
]