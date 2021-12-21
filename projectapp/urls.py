from django.urls import path

from . import views

from projectapp.apis import getTl
from projectapp.apis import cloneV5,cloneV5_5
from projectapp.apis import reports
from projectapp.apis import getSVG
from projectapp.apis import test
from projectapp.apis import cloneSynoptic
from projectapp.apis import clone
from projectapp.apis import factbook
from projectapp.apis import cl
from projectapp.apis import ml
from projectapp.apis import loginMgr
from projectapp.apis import users
from projectapp.apis import energies
from projectapp.apis import alarmes
from projectapp.apis import pdf
from projectapp.apis import dataRequests

urlpatterns = [
    path('', views.index, name='index'),
    # success
    path('api/display/', views._filter),
    path('api/filter/', views._filter),
    path('api/updatedelete/', views.updatedelete),
    #success
 
    path('api/sendid/', views.sendnewid),

    path('api/insertiot/', alarmes.insertiot),
    path('api/getobjective/', alarmes.getobjective),


    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    path("api/clone/",clone.clone),
    path('api/cloneSynopticV2/', cloneSynoptic.cloneSynopticV2),
    path('api/cloneV5/', cloneV5.cloneReport_api),
    path('api/generatePDF/', pdf.generatePDF),
    path('api/getReports/', reports.getReports),
    path('api/getReportById/', reports.getReportById),
    path('api/getReportByName/', reports.getReportByName),

    path('api/getAllCounters/', cl.getAllCounters),
    path('api/getCounters/', cl.getCounters),
    path('api/getCountersList/',cl.getCountersList),
    path('api/getEnergyFromCounters/',cl.getEnergyFromCounters),

    path('api/getMesures/', ml.getMesures),
    path('api/getMLByEnergy/', ml.getMLByEnergy),
    path('api/getMeasureList/', ml.getMeasureList),
    
    path('api/filterPrep/', views.filterPrep),
    path('api/getTl/', getTl.getTl),
    path('api/getTlSql/', getTl.getTlSql),
    path('api/getSVG/', getSVG.getSVG),

    #############Service Cluster################
    path('api/cluster/', dataRequests.cluster),
    #############Service Iot Inner################
    path('api/iotinner/', dataRequests.iotinner),
    path("api/plotUpdate/",dataRequests.plotUpdate ),

    path("api/getFactBooks/",factbook.getFactBooks),
    path("api/getFactBookById/",factbook.getFactBookById),

    path('api/test/', test.testRequest),
    path("api/login/",loginMgr.login),
    path("api/getUser/",users.getUser),

    path("api/getEnergies/",energies.getEnergies),
    path("api/getAllEnergies/",energies.getAllEnergies),

    path("api/getIncidents/",alarmes.getIncidents),

    path('display/', views._filter),
    path('filter/', views._filter),
    path('updatedelete/', views.updatedelete),
    #success
 
    path('sendid/', views.sendnewid),

    path('insertiot/', alarmes.insertiot),
    path('getobjective/', alarmes.getobjective),


    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    #noAtssss
    path("clone/",clone.clone),
    path('cloneSynopticV2/', cloneSynoptic.cloneSynopticV2),
    path('cloneV5/', cloneV5.cloneReport_api),
    path('generatePDF/', pdf.generatePDF),
    path('getReports/', reports.getReports),
    path('getReportById/', reports.getReportById),
    path('getReportByName/', reports.getReportByName),

    path('getAllCounters/', cl.getAllCounters),
    path('getCounters/', cl.getCounters),
    path('getCountersList/',cl.getCountersList),
    path('getEnergyFromCounters',cl.getEnergyFromCounters),
    path('getMesures/', ml.getMesures),
    path('getMLByEnergy/', ml.getMLByEnergy),
    path('getMeasureList/', ml.getMeasureList),
    
    path('filterPrep/', views.filterPrep),
    path('getTl/', getTl.getTl),
    path('getTlSql/', getTl.getTlSql),
    path('getSVG/', getSVG.getSVG),

    #############Service Cluster################
    path('cluster/', dataRequests.cluster),
    #############Service Iot Inner################
    path('iotinner/', dataRequests.iotinner),

    path("getFactBooks/",factbook.getFactBooks),
    path("getFactBookById/",factbook.getFactBookById),

    # path('test/', cloneV5_5.cloneReport_api),
    path("login/",loginMgr.login),
    path("getUser/",users.getUser),

    path("getEnergies/",energies.getEnergies),
    path("getAllEnergies/",energies.getAllEnergies),

    path("getIncidents/",alarmes.getIncidents),
]