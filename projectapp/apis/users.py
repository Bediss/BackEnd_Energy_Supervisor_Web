
from django.http import response
from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getUser(request):
    if request.method == 'GET':
        try:
            if len(request.GET.keys()) >1:
                return HttpResponseBadRequest()

            isValid=jwtVerifyRequest(request)
            if type(isValid) !=dict:
                    return isValid
            userId=isValid["user"]["userId"]
            getHotList=request.GET.get("hl",None)
            db=initDB()
            if getHotList is None:
                user=db.exec("""
                    select row_to_json(t) from (select "User_Master_Name","User_Factbook","User_Report" from "User_Master" where "User_Master_Code"='{}') t
                    """.format(userId))
                user=user[0]
            else:
                user=db.exec("""
                    select row_to_json(t) from (select "User-HotListe" from "User_Master" where "User_Master_Code"='{}') t
                    """.format(userId))
                user=user[0].get("User-HotListe",None)
            db.close()
            if user is None:
                return HttpResponseNotFound()
            resp=JsonResponse(data=user,safe=False)
            resp.status_code=200
            return resp
        except Exception as exp:
            return HttpResponseServerError()

    return HttpResponseBadRequest()
