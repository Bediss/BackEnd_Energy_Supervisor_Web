
from os import stat_result
from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getEnergyFromCounters(request):
    if request.method == "GET":
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                    return isValid
        counters=request.GET.get("counters",None)
        if counters is None:
            return HttpResponseBadRequest()
        counters=counters.split(",")
        output=list()
        for counter in counters:
            ""
            counterData=next((item for item in preps["allCompteurs"] if item["Code_Compteur"]==counter),None)
            out={
                "counter":counter,
                "Energie":None,
            }
            if counterData is not None:
                energy=counterData.get("Energie")
                counterData=next((item for item in preps["EnergyTable"] if item["Name_Energy"]==energy),dict())
                out.update({
                    "Energie":energy,
                    "INP_Prefix_Energy":counterData.get("INP_Prefix_Energy",None),
                    "OUTP_Prefix_Energy":counterData.get("OUTP_Prefix_Energy",None),
                    "Obj_Prefix":counterData.get("Obj_Prefix",None),
                })
            output.append(out)
        return JsonResponse(data=output,safe=False)
    return HttpResponseBadRequest()

def getCounters(request):
    if request.method == 'GET':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
            return isValid
        if "counters" not in request.GET:
            return (HttpResponseBadRequest())
        counters = request.GET.get("counters","")
        params=prepQueryParams(counters.split(","))
        db=initDB()
        resp=db.exec("""
        select array_to_json(array_agg(row_to_json(t))) from (SELECT *FROM public."AllCompteur" where "Code_Compteur" in ({})) t;
        """.format(params["txt"]),params=params["tuple"])
        db.close()

        resp = resp[0]
        
        if resp is None:
            resp=list()
        response=JsonResponse(data=resp,safe=False)
        return response
    return (HttpResponseBadRequest())

def getAllCounters(request):
    if request.method == 'GET':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
            return isValid
        energie=request.GET.get("energie",None)
        db=initDB()
        
        if energie is None:
            resp=db.exec("""
                select array_to_json(array_agg(row_to_json(t)))from (select "Code_Compteur","Le_Compteur","Le_Compteur_Parent",type,"Point_de_Production","Point_de_Distribution","Point_de_Consommation","Energie" from public."AllCompteur") AS t;
            """)
        else:
            energies=list(map(lambda r: '"Energie"=%s', energie.split(',')))
            energie=tuple(map(lambda r: r, energie.split(',')))
            

            energies=' or '.join(energies)
            resp=db.exec("""
                select array_to_json(array_agg(row_to_json(t)))from (select "Code_Compteur","Le_Compteur","Le_Compteur_Parent",type,"Point_de_Production","Point_de_Distribution","Point_de_Consommation","Energie" from public."AllCompteur" where {}) AS t;
            """.format(energies),energie)
        db.close()
        resp=resp[0]
        
        response=dict()
        if resp is None or type(resp) is not list:
            for r in energie:
                response[r]=list()
        else:
            for r in energie:
                filtred=list(filter(lambda e:e["Energie"]==r,resp))
                response[r]=filtred
        res=JsonResponse(data=response,safe=False)
        return res
    return (HttpResponseBadRequest())

def getCountersList(request):
    if request.method=="GET":
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                    return isValid
        counterListId=request.GET.get("counterListId",None)
        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                    password=databasePassword, server=databaseServer, port=databasePort)
        tagSystem=",tag_System" if request.GET.get("ts",None) is not None else ""
        tagUser=",tag_User" if request.GET.get("tu",None) is not None else ""
        if counterListId is None:
            res=tasker.db.exec("""
            select array_to_json(array_agg(row_to_json(t))) from (SELECT "CompteurList_Code","CompteurListI_Name","CL_Membre"{}{} FROM public."CL_V3") t;
            """.format(tagUser,tagSystem))
            res=res[0]
            status=200
        else:
            res=tasker.db.exec("""
            select row_to_json(t) from (SELECT "CompteurList_Code","CompteurListI_Name","CL_Membre"{}{} FROM public."CL_V3") t
            where "CompteurList_Code" = %s
            """.format(tagUser,tagSystem),(counterListId,))
            # status=404 if res is None else 200
            status=200

        tasker.closeConnection()
        resp=JsonResponse(data=res,safe=False)
        resp.status_code=status
        return resp

    return HttpResponseBadRequest()