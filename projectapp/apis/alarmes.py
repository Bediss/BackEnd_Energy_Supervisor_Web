from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getIncidents(request):
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                    return isValid
        id=request.GET.get("id",None)
        db=initDB()
        if id is None:
            resp=db.exec(
            """
            select array_to_json(array_agg(row_to_json(t)))from (select * from "Alarme_F_Reporting_V3") AS t;
            """)
        else:
            alarmes=list(map(lambda r: '"Alarme_Code"=%s', id.split(',')))
            alarmes=' or '.join(alarmes)
            id=tuple(map(lambda r: r, id.split(',')))
            resp=db.exec(
            """
            select array_to_json(array_agg(row_to_json(t)))from (select * from "Alarme_F_Reporting_V3" where {} ) AS t;
            """.format(alarmes),id)
        db.close()
        resp=resp[0]

        res=dict() if resp is None else resp
        res=JsonResponse(data=res,safe=False)
        return res

def insertiot(request):
    if request.method == 'POST':
        try:
            isValid=jwtVerifyRequest(request)
            if type(isValid) !=dict:
                        return isValid
            data = json.loads(request.body)
            datatoinsert = data["datatoinsert"]
            # schema={
            #     "type": "object",               
            # }
            # validate(instance=datatoinsert,schema=schema)
            tasker = Back_worker_bridge(
                rabbitAddress=rabbitServer, rabbitPassword=rabbitUserName, rabbitUsername=rabbitPassword)
            task = {"service": "InsertIot", "data": datatoinsert}
            data = tasker.insert_iot(task)
            tasker.close()
            resp=JsonResponse(data, safe=False)
            return resp
        except Exception as exp:
            return HttpResponseBadRequest()
    return HttpResponseBadRequest()

def getobjective(request):
    if request.method == 'POST':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                    return isValid
        try:
            data = json.loads(request.body)
            schema={
                "type": "object",
                "required": ["ML", "CL"],
                "properties": {
                    "ML": mlSchema,
                    "CL": clSchema,
                }
            }
            validate(instance=data,schema=schema)
        except Exception as exp:
            print(exp)
            return HttpResponseBadRequest()
        ML = data['ML']
        CL = data['CL']
        tasker=initDBB()
        task = {"ml": ML, "cl": CL, "cross_tab": "normalised",
                "retour": "json"}
        data = tasker.cluster(task)
        tasker.closeConnection()
        print(data)
        data = json.dumps(data)
        return HttpResponse(data, content_type='application/json')