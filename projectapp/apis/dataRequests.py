from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest
from projectapp.apis.getTl import _getTl,_getTl2


def iotinner(request):
    if request.method == 'POST':
        try:
            task = json.loads(request.body)
            schema = {
                "type": "object",
                "required": ["ml", "cl", "tl"],
                "properties": {
                    "ml": mlSchema,
                    "cl": clSchema,
                    "tl": tlSchema
                }

            }
            validate(instance=task, schema=schema)
        except Exception as exp:
            print(exp)
            return HttpResponseBadRequest()
        tl = task.get("tl", None)
        if type(tl) is str:
            tlSQL = json.loads(_getTl(tl, "Tl_Sql"))
            
            task["tl"] = tlSQL.get(tl, dict()).get("tl", None)
       
        tasker = Back_worker_bridge(
            rabbitAddress=rabbitServer, rabbitPassword=rabbitPassword, rabbitUsername=rabbitUserName)

        data = tasker.iot_inner(task)
        tasker.close()
        return JsonResponse(data=data,safe=False)
    return HttpResponseBadRequest()



def cluster(request):
    if request.method == 'POST':

        try:
            task = json.loads(request.body)
            schema = {
                "type": "object",
                "required": ["ml", "cl"],
                "properties": {
                    "ml": mlSchema,
                    "cl": clSchema,
                    "tl": tlSchema
                }

            }
            validate(instance=task, schema=schema)
        except Exception as exp:
            print(exp)
            return HttpResponseBadRequest()
        tasker=initDBB()
        if task.get("tl", False):  
            tt = _getTl(task.get("tl", ""), "Tl_Sql")
            tt = json.loads(tt)
            task.update({"tl": tt.get(task.get("tl"), dict()).get("tl", list())})

        data = tasker.cluster(task)
        tasker.closeConnection()
        if data == 400:
            return HttpResponseBadRequest()
        return JsonResponse(data=data,safe=False)
    return HttpResponseBadRequest()



def handleTl(time=""):
    return json.loads("""
    [{
        "SQL": " iot.date between now() - interval 'TEMPLATE' and now()",
        "SQLc": "where asc"
      }
      ,
      {
        "SQL": "time_bucket('TEMPLATE', iot.date) AS time,LOCF(max(iot.value))as valeur",
        "SQLc": "select"
    }
    ]
    """.replace("\n","").strip().replace("TEMPLATE",time))



def plotUpdate(request):
    if request.method == 'POST':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                    return isValid
        try:
            task=json.loads(request.body)
            schema = {
                "type": "object",
                "required": ["ml", "cl"],
                "properties": {
                    "ml": mlSchema,
                    "cl": clSchema,
                    "tl": tlSchema
                }
            }
            validate(instance=task,schema=schema)
        except:
            return HttpResponseBadRequest()
        
        tl=task.get("tl",None)
        tlSQL=None
        tlUser=None
        data=dict()
        if tl is None:
            db=initDBB()
            data["data"]=db.cluster(task)
            db.closeConnection()
        else:
            tlUser = json.loads(_getTl2(tl, "Tl_User",True))
            
            interval=next((item for item in tlUser.get(tl,dict()).get("tl",list()) if item.get("operateur").lower()=='intervalle'),dict()).get("valeur",None)
            if interval:
                interval=interval.replace("minute","min").replace("jour","day").replace("jours","day")
                tlSQL=handleTl(interval)
                task["tl"]=tlSQL
                tasker = Back_worker_bridge(
                rabbitAddress=rabbitServer, rabbitPassword=rabbitPassword, rabbitUsername=rabbitUserName)
                data["data"]= tasker.iot_inner(task)
                tasker.close()
                data["maxPlotPoint"]=tlUser.get(tl,dict()).get("tlConfig",dict()).get("maxPlotPoint",None)
            else:
                return HttpResponseBadRequest()
        return JsonResponse(data=data,safe=False)
    return HttpResponseBadRequest()