

from .common import *

def getTl(request):
    if request.method == "GET":
        param = request.GET.get("id", None)
        target=None if request.GET.get("u", None) is None else 'Tl_User'
        res= _getTl2(param, target)

        if res == 400:
            return HttpResponseBadRequest()
        elif res==404:
            return HttpResponseNotFound()
        else:
            return HttpResponse(res)
    return HttpResponseBadRequest()


def getTlSql(request):
        if request.method == "GET":
            param = request.GET.get("id", None)
            res=_getTl(param, "Tl_Sql")
            if res == 400:
                return HttpResponseBadRequest()
            elif res==404:
                return HttpResponseNotFound()
            else:
                return HttpResponse(res)
        return HttpResponseBadRequest()

def _getTl(param, target):

    db = Back_DB_bridge(database=databasename, username=databaseUser,
                        password=databasePassword, server=databaseServer, port=databasePort)
    if target is None:
        return 400
    if param is None:
        resp=db.db.exec('''select json_object_agg(tl_id,json_build_object('name', tl_name,'type',
											   (case
											   		when tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQLc'='limit' and tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQL' like '%limit 1%' then 'cluster'
											   		when tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQLc'='limit' and tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQL' like '%limit 1%'  then 'cluster'
											   		when tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQLc'='select' and tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQL' like '%bucket%' then 'iot_value'
											   		when tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQLc'='select' and tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQL' like '%bucket%'then 'iot_value'
													else NULL
											   end)
											   , 'tl', tl_members -> 0 -> '{}')) from public.tl'''.format(target))
    else:
        if re.search("[^0-9a-zA-Z]+", param) is not None:
            return 400
        tl = db.db.exec(
            '''select json_object_agg(tl_id,json_build_object('name', tl_name,'type',
            (case
                when tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQLc'='limit' and tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQL' like '%limit 1%' then 'cluster'
				when tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQLc'='limit' and tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQL' like '%limit 1%'  then 'cluster'
				when tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQLc'='select' and tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQL' like '%bucket%' then 'iot_value'
				when tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQLc'='select' and tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQL' like '%bucket%'then 'iot_value'
				else NULL
            end)
            , 'tl', tl_members -> 0 -> '{}')) from public.tl where tl_id='{}' 
            '''.format(target, param))
        resp = tl[0] if type(tl) is tuple else tl
    return json.dumps(resp if resp is not None else list() if param is None else dict())

def _getTl2(param, target):
    
    db = Back_DB_bridge(database=databasename, username=databaseUser,
                        password=databasePassword, server=databaseServer, port=databasePort)

    if param is None:
        res=db.db.exec('''
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
                # res=r
        resp=dict()
        resp[res[0][0]]=res[0][1]
        resp[res[1][0]]=res[1][1]
    else:
        if re.search("[^0-9a-zA-Z]+", param) is not None:
            return 400
        tl = db.db.exec(
            '''select json_object_agg(tl_id,json_build_object('name', tl_name,'type',
            (case
                when tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQLc'='limit' and tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQL' like '%limit 1%' then 'cluster'
				when tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQLc'='limit' and tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQL' like '%limit 1%'  then 'cluster'
				when tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQLc'='select' and tl_members -> 0 -> 'Tl_Sql' -> 1 ->>'SQL' like '%bucket%' then 'iot_value'
				when tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQLc'='select' and tl_members -> 0 -> 'Tl_Sql' -> 0 ->>'SQL' like '%bucket%'then 'iot_value'
				else NULL
            end)
            {})) from public.tl where tl_id='{}' 
            '''.format("" if target is None else ", 'tl', tl_members -> 0 -> '{}'".format(target), param))
        resp = tl[0] if type(tl) is tuple else tl
    return json.dumps(resp if resp is not None else list() if param is None else dict())
