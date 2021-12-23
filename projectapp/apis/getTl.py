

from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getTl(request):
    isValid=jwtVerifyRequest(request)
    if type(isValid) !=dict:
                return isValid
    if request.method == "GET":
        param = request.GET.get("id", None)
        target=None if request.GET.get("u", None) is None else 'Tl_User'
        res= _getTl2(param, target)
        if res == 400:
            return HttpResponseBadRequest()
        elif res==404:
            return HttpResponseNotFound()
        else:
            return JsonResponse(data=json.loads(res),safe=False)
    return HttpResponseBadRequest()

def getTlSql(request):
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                return isValid
        if request.method == "GET":
            param = request.GET.get("id", None)
            res=_getTl(param, "Tl_Sql")
            if res == 400:
                return HttpResponseBadRequest()
            elif res==404:
                return HttpResponseNotFound()
            else:
                return JsonResponse(data=json.loads(res),safe=False)
        return HttpResponseBadRequest()

def clusterOrIot(Tl_Sql):
    ""
    cluster="cluster" if ((Tl_Sql[0].get("SQLc","")=="limit" and Tl_Sql[0].get("SQL","").find("limit 1")!=-1) \
                or (Tl_Sql[1].get("SQLc","")=="limit" and Tl_Sql[1].get("SQL","").find("limit 1")!=-1)) is True else False

    iot='iot_inner' if ((Tl_Sql[0].get("SQLc","")=="select" and Tl_Sql[0].get("SQL","").find("bucket")!=-1) \
                or (Tl_Sql[1].get("SQLc","")=="select" and Tl_Sql[1].get("SQL","").find("bucket")!=-1) ) else False
    return iot or cluster

def _getTl(param, target):
    
    if target is None:
        return 400
    if param is None:
        resp=list()
        for item in preps.get("tl",list()):
            id=item.get("tl_id")
            d=dict()
            d["name"]=item.get("tl_name")
            Tl_Sql=item.get("tl_members",list(dict()))[0].get("Tl_Sql")

            
            d["type"] =clusterOrIot(Tl_Sql)
            if target == 'Tl_Sql':
                d["tl"]=Tl_Sql
            else:
                d["tl"]=item.get("tl_members",list(dict()))[0].get("Tl_User")
            resp=dict({id:d})

    else:
        if re.search("[^0-9a-zA-Z]+", param) is not None:
            return 400
        d=dict()
        _tl=next((tl for tl in preps.get("tl",list()) if tl["tl_id"] == param),None)
        Tl_Sql=_tl.get("tl_members",list(dict()))[0].get("Tl_Sql")

        d["type"] =clusterOrIot(Tl_Sql)
        d["name"]=_tl.get("tl_name")
        # d["tl"]=""
        if target == 'Tl_Sql':
            d["tl"]=Tl_Sql
        else:
            d["tl"]=_tl.get("tl_members",list(dict()))[0].get("Tl_User")
       
        resp=dict({param:d})
    return json.dumps(resp if resp is not None else list() if param is None else dict())

def _getTl2(param, target,getConfig=False):
    resp=dict()
    if param is None:
        cluster=list()
        iot=list()
        for _tl in preps.get("tl",list()):
            d=dict()
            Tl_Sql=_tl.get("tl_members",list(dict()))[0].get("Tl_Sql")
            d["name"]=_tl["tl_name"]
            d["code"]=_tl["tl_id"]
            r=clusterOrIot(Tl_Sql)

            if target:
                d["tl"]=_tl.get("tl_members",list(dict()))[0].get(target)

            if r=='cluster':
                cluster.append(d)
            elif r=='iot_inner':
                iot.append(d)
            
        resp={
            "cluster":cluster,
            "iot_inner":iot
        }

    else:

        if re.search("[^0-9a-zA-Z]+", param) is not None:
            return 400
        d=dict()

        _tl=next((tl for tl in preps.get("tl",list()) if tl["tl_id"] == param),None)
        Tl_Sql=_tl.get("tl_members",list(dict()))[0].get("Tl_Sql")

        d["name"]=_tl["tl_name"]
        d["type"]=clusterOrIot(Tl_Sql)
        if getConfig:
            d["tlConfig"]=_tl["tlConfig"]
        if target:
            d["tl"]=_tl.get("tl_members",list(dict()))[0].get(target)
        resp=dict({
            param:d
        })

    return json.dumps(resp if resp is not None else list() if param is None else dict())
