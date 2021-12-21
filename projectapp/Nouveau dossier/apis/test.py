


from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest
from django.http.response import HttpResponseNotFound
from back_DB_bridge import Back_DB_bridge
import re
import json
from .common import *



def testRequest(request):
    tasker = Back_DB_bridge(database=databasename,username=databaseUser,password=databasePassword,server=databaseServer,port=databasePort)
    # target='tl_User' if request.GET.get("u", None) is not None else None
    target=None if request.GET.get("u", None) is None else 'Tl_User'
    resp=tasker.db.exec('''
                        SELECT (case
                                    when tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQLc'='limit' and tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQL' like '%limit 1%' then 'cluster'
                                    when tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQLc'='limit' and tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQL' like '%limit 1%'  then 'cluster'
                                    when tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQLc'='select' and tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQL' like '%bucket%' then 'iot_value'
                                    when tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQLc'='select' and tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQL' like '%bucket%'then 'iot_value'
                                    else NULL
                                end),
                                    json_agg(json_build_object('name', tl_name, 'code' , tl_id{}))	  
                        FROM public.tl
                        GROUP BY 1;
                '''.format(",'tl',tl_members -> 0 ->'{}'".format(target)if target is not None else ""),many=True)
    res=dict()
    res[resp[0][0]]=resp[0][1]
    res[resp[1][0]]=resp[1][1]
    
    return HttpResponse(json.dumps(res))