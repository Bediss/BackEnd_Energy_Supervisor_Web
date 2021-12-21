
from projectapp.apis.common import *
import copy
from projectapp.apis.getTl import _getTl
from jsonschema import validate

import requests
import json


from rest_framework.permissions import IsAuthenticated

# Create your views here.

preps = dict({
    "energies": list(),
    "levels": list(),
    "types": list(),
    "mls": list()
})

def index(request):
    return HttpResponse("ok")


def display(request):
    ###############################################
    data = []
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data['tablename']
        fields = data['fields']
        content = data['content']

        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                password=databasePassword, server=databaseServer, port=databasePort)

        task = {"Table_name": tablename, "Header_list": fields, "Header_value": content,
                "Column_select_liste": "", "Column_condition_select_list": "", "Column_orderby_list": ""}
        data = tasker.display(task)
        data = data[0]

        data = json.dumps(data)
    return HttpResponse(data, content_type="application/json")

def _filter(request):
    data = []
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data.get('tablename', "")
        fields = data.get('fields', "")
        content = data.get('content', "")
        identifier = data.get('identifier', "")
        typefile = "json"
        dataselect = data.get('dataselect', "")
        dist = data.get('dist', "")
        orderby = data.get('orderby', "")

        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)

        task = {"Table_name": tablename,
                "Header_list": fields,
                "Header_value": content,
                "Column_select_liste": dataselect,
                "Column_condition_select_list": dist,
                "Column_orderby_list": orderby}
 
        data = tasker.display(task)

        data = data[0] if type(data) is tuple and len(data) > 0 else tuple()
        # data = data[0]
        output = list()
        if data is not None and data != "null":
            for el in data:
                if el.get("Report_Code", "").find("UDR") == -1:
                    output.append(el)

        data = json.dumps(copy.deepcopy(output))

    return HttpResponse(data, content_type="application/json")

def filterPrep(request):
    if request.method == 'GET':
        _energies = "energies" if "energies" in request.GET else False
        _level = "levels" if "levels" in request.GET else False
        _type = "types" if "types" in request.GET else False
        toGet = list(filter((lambda e: e is not False),
                     [_energies, _level, _type]))
        if len(toGet) > 0:
            data = dict()
            for elem in toGet:
                data[elem] = preps[elem]
            return(HttpResponse(json.dumps(data), content_type="application/json"))

        return HttpResponseBadRequest()
    return HttpResponseBadRequest()

def getMLByEnergy(request):
    if request.method == 'GET':
        energies = request.GET.get("energies", None)
        if energies is None:
            return HttpResponseBadRequest()

        energies = [s.capitalize() for s in energies.split(",")]
        out = dict()

        for ml in copy.deepcopy(preps["EnergyMeasureNormalised"]):
            measure_Energy = ml["measure_Energy"]
            if measure_Energy in energies:

                ml.pop("measure_Energy")
                if measure_Energy not in out:
                    out[measure_Energy] = list()
                out[measure_Energy].append(ml)

        return(HttpResponse(json.dumps(out), content_type="application/json"))

def updatedelete(request):
    data = []
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data['tablename']
        identifier = data['identifier']

        datatomodified = data['datatomodified']

        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort,transaction=True)
        task = {
            "Table_name": tablename,
            "data": datatomodified
        }
        data = tasker.update(task)

    return(HttpResponse(data, content_type="application/json"))

def sendnewid(request):
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data['tablename']
        nombreid = data['nombermaxcode']
        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)
        task = {"Table_name": tablename, "nbr_code": nombreid}

        resp = tasker.getMaxCode(task)
        
        resp = resp.replace('[', '')
        resp = resp.replace(']', '')

    return HttpResponse(resp)

def insertiot(request):
    data = {'status': 'fail'}
    if request.method == 'POST':
        data = json.loads(request.body)
        datatoinsert = data["datatoinsert"]
        tasker = Back_worker_bridge(
            rabbitAddress=rabbitServer, rabbitPassword=rabbitUserName, rabbitUsername=rabbitPassword)
        task = {"service": "InsertIot", "data": datatoinsert}
        data = tasker.insert_iot(task)
        tasker.close()
    return(JsonResponse(data, safe=False))

def getobjective(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        ML = data['ML']
        CL = data['CL']
        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)
        task = {"ml": ML, "cl": CL, "cross_tab": "normalised",
                "retour": "json"}
        data = tasker.cluster(task)
        data = json.dumps(data)
    return(HttpResponse(data))

def getobjectivetest(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        ML = data['ML']
        CL = data['CL']
        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)
        task = {"ml": ML, "cl": CL, "cross_tab": "normalised",
                "retour": "json"}
        data = tasker.cluster(task)
        data = json.dumps(data)
    return HttpResponse(data, content_type='application/json')

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
        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)
        if task.get("tl", False):
   
            tt = _getTl(task.get("tl", ""), "Tl_Sql")
            tt = json.loads(tt)
            task.update({"tl": tt.get(task.get("tl"), dict()).get("tl", list())})

        data = tasker.cluster(task)

        if data == 400:
            return HttpResponseBadRequest()
        return HttpResponse(json.dumps(data))
    return HttpResponseBadRequest()

def servicecluster(task):

    tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)

    # task = {"ml": ML, "cl": CL, "cross_tab": "normalised",
    #         "retour": "json"}

    data = tasker.cluster(task)

    return(data)

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
        if tl is None:
            return HttpResponseBadRequest()
        if type(tl) is str:
            tlSQL = json.loads(_getTl(tl, "Tl_Sql"))
            task["tl"] = tlSQL.get(tl, dict()).get("tl", None)

        tasker = Back_worker_bridge(
            rabbitAddress=rabbitServer, rabbitPassword=rabbitPassword, rabbitUsername=rabbitUserName)
        data = tasker.iot_inner(task)
        tasker.close()
        return HttpResponse(json.dumps(data), content_type="application/json")
    return HttpResponseBadRequest()

def getCounters(request):
    if request.method == 'GET':
        if "counters" not in request.GET:
            return (HttpResponseBadRequest())
        counters = request.GET["counters"]
        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)

        task = {"Table_name": "AllCompteur",
                "Header_list": "Code_Compteur",
                "Header_value": counters,
                "Column_select_liste": "",
                "Column_condition_select_list": "*",
                "Column_orderby_list": "asc"}

        resp = tasker.display(task)
        resp = resp[0]

        if resp is None:
            return (HttpResponseNotFound())

        response = HttpResponse()
        response.write(json.dumps(resp))
        response["Access-Control-Allow-Origin"] = "*"
        response["content-type"] = "application/json"
        return response
    return (HttpResponseBadRequest())

def getMesures(request):

    if request.method == 'GET':
        if "mesures" not in request.GET:
            return (HttpResponseBadRequest())
        mesures = request.GET["mesures"]

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



def aa(request):
    ""
initPrep()