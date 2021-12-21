
from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getReportById(request):
    if request.method == 'GET':
        
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
            return isValid

        if "reportId" not in request.GET:
            return (HttpResponseBadRequest())
        reportId = request.GET["reportId"]
        userId=isValid["user"]["userId"]
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
        db=initDB()
  
        db.exec("""
            UPDATE public."User_Master" SET "User-HotListe" = jsonb_path_query_array('[%s]' || "User-HotListe" - '%s', '$[0 to 9]')
            where "User_Master_Code"='%s';
        """,params=(reportId,reportId,userId))
        db.close()
        # response=JsonResponse(data=resp)
        # return response

        response = JsonResponse(data=resp,safe=False)

        response["Access-Control-Allow-Origin"] = "*"
        # response["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        response["Access-Control-Max-Age"] = "1000"
        response["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type, token"

        return response
        return HttpResponse(json.dumps(resp), content_type="application/json",headers={"Access-Control-Allow-Origin":"*"})

    return (HttpResponseBadRequest())

def getReportByName(request):
    if request.method == 'GET':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
            return isValid
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
        

        response=JsonResponse(data=resp,safe=False)
        # response = HttpResponse()
        # response.write(json.dumps(resp))
        # response["Access-Control-Allow-Origin"] = "*"
        # response["content-type"] = "application/json"
        return response

    return (HttpResponseBadRequest())

def getReports(request):
    if request.method == 'GET':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
            return isValid
        try:
            reportsIds=request.GET.get("reportsIds",None)
            sort=request.GET.get("sort",'asc')
            if sort.lower() not in ["asc","desc"]:
                return HttpResponseBadRequest()
        except:
            return HttpResponseBadRequest()

        getBody=',"Body"' if request.GET.get("b",None) is not None else ""
        getSelectedGlobals=',"Selected_Global"' if request.GET.get("g",None) is not None else ""
        
        db = initDB()

        if reportsIds:
            rIds=copy.deepcopy(reportsIds).split(",")
            params=prepQueryParams(rIds)
            # _resp=db.exec("""
            # select array_to_json(array_agg(row_to_json(t))) from (SELECT "Report_Code","Report_Name","Report_Description","Report_TableauName","Report_Master","TAGS"{}{} FROM public."Reporting_V3" where "Report_Code" in ({})) t;
            # """.format(getBody,getSelectedGlobals,params["txt"]),params["tuple"])


            _resp=db.exec(
            """
            select array_to_json(array_agg(row_to_json(t))) from (SELECT "Report_Code","Report_Name","Report_Description","Report_TableauName","Report_Master","TAGS"{}{},(
            case 
                when "Body" ? 'objects' then 'report'
                when "Body" ? 'object' then 'synoptic'
                else NULL
            end
            ) as "type" FROM public."Reporting_V3" where "Report_Code" in ('DDR01')) t;
            """.format(getBody,getSelectedGlobals,params["txt"]),params["tuple"])

        else:
            # _resp=db.exec("""
            # select array_to_json(array_agg(row_to_json(t))) from (SELECT "Report_Code","Report_Name","Report_Description","Report_TableauName","Report_Master","TAGS"{}{} FROM public."Reporting_V3") t;
            # """.format(getBody,getSelectedGlobals))
            _resp=db.exec("""
            select array_to_json(array_agg(row_to_json(t))) from (SELECT "Report_Code","Report_Name","Report_Description","Report_TableauName","Report_Master","TAGS"{}{}
            ,(
            case 
                when "Body" ? 'objects' then 'report'
                when "Body" ? 'object' then 'synoptic'
                else NULL
            end
            ) as "type"
             FROM public."Reporting_V3") t;
            """.format(getBody,getSelectedGlobals))
        db.close()
        _resp=_resp[0]
        if _resp is not None:
            _resp=list(filter(lambda e:e["Report_Code"].find("UDR")==-1,_resp))
            if reportsIds:
                out=list()
                for rId in rIds:
                    out.append(next((item for item in _resp if item["Report_Code"] == rId),None))
                _resp=out

            resp=JsonResponse(data={"count":len(_resp) ,"reports":_resp},safe=False)
            resp.status_code=200
        else:
            resp=JsonResponse(data=None,safe=False)
            resp.status_code=200
        return resp
    return HttpResponseBadRequest()

def saveReport(request):
    if request.method == 'PUT':
        try:
            isValid=jwtVerifyRequest(request)
            if type(isValid) !=dict:
                return isValid
            data = json.loads(request.body)
        except:
            return HttpResponseBadRequest()
        tasker=initDBB(transaction=True)
        data["DBAction"]= 2
        data["Report_Code"]=tasker.getMaxCode()
        task = {
            "Table_name": 'Reporting_V3',
            "data": data
        }
        # tasker=Back_DB_bridge(database=databasename, username=databaseUser, password=databasePassword, server=databaseServer, port=databasePort,transaction=True)
        
        res=tasker.update(task)
        tasker.closeConnection()
        if "op" in res:
                return JsonResponse(data=res,safe=False)
        else:
            return HttpResponseServerError()

    return HttpResponseBadRequest()

def updateReport(request):
    if request.method == 'PATCH':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
            return isValid
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
