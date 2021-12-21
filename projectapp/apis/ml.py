
from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getMLByEnergy(request):
    isValid=jwtVerifyRequest(request)
    if type(isValid) !=dict:
            return isValid
    if request.method == 'GET':
        energies = request.GET.get("energies", None)
        if energies is None:
            return HttpResponseBadRequest()

        energies = [s for s in energies.split(",")]
        out = dict()

        for ml in copy.deepcopy(preps["EnergyMeasureNormalised"]):
            measure_Energy = ml["measure_Energy"]
            if measure_Energy in energies:

                ml.pop("measure_Energy")
                if measure_Energy not in out:
                    out[measure_Energy] = list()
                out[measure_Energy].append(ml)
        return JsonResponse(data=out,safe=False)

def getMesures(request):
    if request.method == 'GET':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                return isValid
        mesures=request.GET.get("mesures",None)
        if mesures is None:
            return (HttpResponseBadRequest())

        mesures = mesures.strip()
        if mesures == "":
            return (HttpResponseBadRequest())

        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)
        task = {"Table_name": "EnergyMeasureNormalised",
                "Header_list": "EMNCode",
                "Header_value": mesures,
                "Column_select_liste": "",
                "Column_condition_select_list": "*",
                "Column_orderby_list": "asc"}
        resp = tasker.display(task)
        resp = resp[0]
        response = HttpResponse()
        response.write(json.dumps(resp))
        response["Access-Control-Allow-Origin"] = "*"
        response["content-type"] = "application/json"
        return response
    return HttpResponseBadRequest

def getMeasureList(request):
    
    if request.method=="GET":
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                return isValid
        measureListId=request.GET.get("measureListId",None)
        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)
        tagSystem=",tag_system" if request.GET.get("ts",None) is not None else ""
        tagUser=",tag_user" if request.GET.get("tu",None) is not None else ""
        if measureListId is None:
            res=tasker.db.exec("""
            select array_to_json(array_agg(row_to_json(t))) from (SELECT "ML_Code","ML_Name","ML_Membre"{}{} FROM public."ML_V3") t;
            """.format(tagUser,tagSystem))
            res=res[0]
            status=200
        else:
            res=tasker.db.exec("""
            select row_to_json(t) from (SELECT "ML_Code","ML_Name","ML_Membre"{}{} FROM public."ML_V3") t
            where "ML_Code" = %s;
            """.format(tagUser,tagSystem,),(measureListId,))
            status=404 if res is None else 200

        tasker.closeConnection()
        resp=JsonResponse(data=res,safe=False)
        resp.status_code=status
        return resp

    return HttpResponseBadRequest()