
from .common import *

def getReportById(request):
    if request.method == 'GET':
        if "reportId" not in request.GET:
            return (HttpResponseBadRequest())
        reportId = request.GET["reportId"]

        if reportId.find("UDR") == 0:
            return HttpResponseNotFound()

        getBody="" if request.GET.get("b",None) is None else "Body;" 
        getSelectedGlobals="" if request.GET.get("g",None) is None else "Selected_Global;" 
        getName="" if request.GET.get("n",None) is None else "Report_Name;" 
        getDescription="" if request.GET.get("d",None) is None else "Report_Description;" 
        getTableName="" if request.GET.get("tn",None) is None else "Report_TableauName;" 
        getTableCode="" if request.GET.get("tc",None) is None else "Report_TableauCode;" 
        getReportMaster="" if request.GET.get("m",None) is None else "Report_Master;" 
        getTags="" if request.GET.get("t",None) is None else "TAGS;" 
        getDisposition="" if request.GET.get("di",None) is None else "disposition;" 
        tasker = Back_DB_bridge(database=databasename,username=databaseUser,password=databasePassword,server=databaseServer,port=databasePort)
        task = {"Table_name": "Reporting_V3",
                "Header_list": "Report_Code",
                "Header_value": reportId,
                "Column_select_liste": "Report_Code;{}{}{}{}{}{}{}{}{}".format(getBody,getSelectedGlobals,getName,getDescription,getTableName,getTableCode,getReportMaster,getTags,getDisposition),
                "Column_condition_select_list": "*",
                "Column_orderby_list": "asc"}
        
        if task["Column_select_liste"][-1] ==";":
            task["Column_select_liste"]=task["Column_select_liste"][:-1]

        resp = tasker.display(task)
        tasker.closeConnection()
        resp = resp[0]

        if resp is None:
            return (HttpResponseNotFound())
        
        resp = resp.pop()
        response = HttpResponse()
        response.write(json.dumps(resp))
        response["Access-Control-Allow-Origin"] = "*"
        response["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        response["Access-Control-Max-Age"] = "1000"
        response["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type"
        response["content-type"] = "application/json"
        return response
        # return HttpResponse(json.dumps(resp), content_type="application/json",headers={"Access-Control-Allow-Origin":"*"})

    return (HttpResponseBadRequest())

def getReportByName(request):
    if request.method == 'GET':
        if "reportName" not in request.GET:
            return (HttpResponseBadRequest())
        reportName = request.GET["reportName"]
        getBody="" if request.GET.get("b",None) is None else "Body;" 
        getSelectedGlobals="" if request.GET.get("g",None) is None else "Selected_Global;" 

        getDescription="" if request.GET.get("d",None) is None else "Report_Description;" 
        getTableName="" if request.GET.get("tn",None) is None else "Report_TableauName;" 
        getTableCode="" if request.GET.get("tc",None) is None else "Report_TableauCode;" 
        getReportMaster="" if request.GET.get("m",None) is None else "Report_Master;" 
        getTags="" if request.GET.get("t",None) is None else "TAGS;" 
        getDisposition="" if request.GET.get("di",None) is None else "disposition;" 
        tasker = Back_DB_bridge(database=databasename,username=databaseUser,password=databasePassword,server=databaseServer,port=databasePort)
        task = {"Table_name": "Reporting_V3",
                "Header_list": "Report_Name",
                "Header_value": reportName,
                "Column_select_liste": "Report_Code;Report_Name;{}{}{}{}{}{}{}{}".format(getBody,getSelectedGlobals,getDescription,getTableName,getTableCode,getReportMaster,getTags,getDisposition),
                "Column_condition_select_list": "*",
                "Column_orderby_list": "asc"}

        if task["Column_select_liste"][-1] ==";":
                task["Column_select_liste"]=task["Column_select_liste"][:-1]
                
        resp = tasker.display(task)
        tasker.closeConnection()
        resp = resp[0]
        if resp is None:
            return (HttpResponseNotFound())
        resp = resp.pop()
        if resp.get("Report_Code", "").find("UDR") == 0:
            return HttpResponseNotFound()
        
        response = HttpResponse()
        response.write(json.dumps(resp))
        response["Access-Control-Allow-Origin"] = "*"
        response["content-type"] = "application/json"
        return response

    return (HttpResponseBadRequest())

def getReports(request):
    if request.method == 'GET':
        try:

            page=request.GET.get("page",None)
            pageSize=request.GET.get("pageSize",None)
            sort=request.GET.get("sort",'asc')
            if sort.lower() not in ["asc","desc"]:
                return HttpResponseBadRequest()
            # page=int(page)
            # pageSize=int(pageSize)
        except:
            return HttpResponseBadRequest()
        if page is not None and page <1:
            return HttpResponseBadRequest()
        getBody=request.GET.get("b",None)
        getSelectedGlobals=request.GET.get("g",None)
        
        tasker = Back_DB_bridge(database=databasename,username=databaseUser,password=databasePassword,server=databaseServer,port=databasePort)
        task = {"Table_name": "Reporting_V3",
                "Header_list": "*",
                "Header_value": '*',
                "Column_select_liste": "Report_Code;Report_Name;Report_Description;Report_TableauName;Report_Master;TAGS;{}{}disposition".format("" if getBody is None else "Body;","" if getSelectedGlobals is None else "Selected_Global;"),
                "Column_condition_select_list": "*",
                "Column_orderby_list": "{}".format(sort)}
        resp = tasker.display(task)
        tasker.closeConnection()
        resp=resp[0]
        resp=list(filter(lambda e:e["Report_Code"].find("UDR")==-1,resp))
        count=len(resp)
        if page == None:
            data={"reports":resp,"count":count}
        else:
            # todo (without display)
            resp=resp[(page-1) * pageSize : page * pageSize]
            data={"count":len(resp),"reports":resp}
            # tasker.db.exec("select Report_Code,Report_Name,Report_TableauName,Report_Master,TAGS from Reporting_V3")
            
        return HttpResponse(json.dumps(data))
    return HttpResponseBadRequest()

def saveReport(request):
    if request.method == 'PUT':
        data = json.loads(request.body)
        data["DBAction"]= 2
        task = {
            "Table_name": 'Reporting_V3',
            "data": data
        }
        tasker=Back_DB_bridge(database=databasename, username=databaseUser, password=databasePassword, server=databaseServer, port=databasePort,transaction=True)
        res=tasker.update(task)
        tasker.closeConnection()
        if "op" in res:
            if res["op"] == "ok":
                return HttpResponse(json.dumps(res))
            else:
                return HttpResponseServerError()

    return HttpResponseBadRequest()
def updateReport(request):
    if request.method == 'PUT':
        data = json.loads(request.body)
        data["DBAction"]= 1
        task = {
            "Table_name": 'Reporting_V3',
            "data": data
        }
        tasker=Back_DB_bridge(database=databasename, username=databaseUser, password=databasePassword, server=databaseServer, port=databasePort,transaction=True)
        res=tasker.update(task)
        tasker.closeConnection()
        if "op" in res:
            if res["op"] == "ok":
                return HttpResponse(json.dumps(res))
            else:
                return HttpResponseServerError()

    return HttpResponseBadRequest()