
from projectapp.apis.common import *
import copy
from projectapp.apis.getTl import _getTl
from jsonschema import validate

import requests
import json

# Create your views here.


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
        tasker=initDBB()
        # tasker = Back_DB_bridge(database=databasename, username=databaseUser,
        #                         password=databasePassword, server=databaseServer, port=databasePort)

        task = {"Table_name": tablename, "Header_list": fields, "Header_value": content,
                "Column_select_liste": "", "Column_condition_select_list": "", "Column_orderby_list": ""}
        data = tasker.display(task)
        tasker.closeConnection()
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
        tasker=initDBB()
        # tasker = Back_DB_bridge(database=databasename, username=databaseUser,
        #                         password=databasePassword, server=databaseServer, port=databasePort)

        task = {"Table_name": tablename,
                "Header_list": fields,
                "Header_value": content,
                "Column_select_liste": dataselect,
                "Column_condition_select_list": dist,
                "Column_orderby_list": orderby}
 
        data = tasker.display(task)
        tasker.closeConnection()
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

def updatedelete(request):
    data = []
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data['tablename']
        identifier = data['identifier']

        datatomodified = data['datatomodified']
        tasker=initDBB(transaction=True)
        # tasker=Back_DB_bridge(database=databasename, username=databaseUser,
        #                                 password=databasePassword, server=databaseServer, port=databasePort,transaction=True)
        task = {
            "Table_name": tablename,
            "data": datatomodified
        }
        data = tasker.update(task)
        tasker.closeConnection()

    return(HttpResponse(data, content_type="application/json"))

def sendnewid(request):
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data['tablename']
        nombreid = data['nombermaxcode']
        tasker=initDBB()
        # tasker = Back_DB_bridge(database=databasename, username=databaseUser,
        #                         password=databasePassword, server=databaseServer, port=databasePort)
        task = {"Table_name": tablename, "nbr_code": nombreid}

        resp = tasker.getMaxCode(task)
        tasker.closeConnection()
        resp = resp.replace('[', '')
        resp = resp.replace(']', '')

    return HttpResponse(resp)

def servicecluster(task):

    # tasker = Back_DB_bridge(databasename)
    tasker=initDBB()

    # task = {"ml": ML, "cl": CL, "cross_tab": "normalised",
    #         "retour": "json"}

    data = tasker.cluster(task)
    tasker.closeConnection()
    return(data)

initPrep()

initUsers()

listeners()