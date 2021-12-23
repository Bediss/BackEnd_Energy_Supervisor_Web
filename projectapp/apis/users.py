
from django.http import response
from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getUser(request):
    if request.method == 'GET':
        try:
            isValid=jwtVerifyRequest(request)
            if type(isValid) !=dict:
                    return isValid
        except:
            return HttpResponseBadRequest()
        try:
            userId=isValid["user"]["userId"]
            getHotList=request.GET.get("hl",None)

            if getHotList is None:
                _user=next((user for user in getUsers() if user.get("User_Master_Code",None)==userId),dict())
                user=dict({
                    "User_Master_Name":_user.get("User_Master_Name",""),
                    "User_Factbook":_user.get("User_Factbook",""),
                    "User_Report":_user.get("User_Report",""),
                    "userType":_user.get("userType","")
                })
                user["userType"]=user.get("userType","").split("|")
                resp=user
            else:
                db=initDB()
                hl=db.exec("""
                        select array_to_json(array_agg(row_to_json(t)))from (
                        select "Report_Code", "Report_Name" 
                        from public."Reporting_V3" r 
                        inner join 
                        (
                        select unnest("User-HotListe") coder
                        from public."User_Master" 
                        where "User_Master_Code"=%s) s1
                        on s1.coder=r."Report_Code"
                        ) AS t;
                """,(userId,))
                db.close()
                hl=list(hl).pop()
                resp={"hotList":hl}
            resp=JsonResponse(data=resp,safe=False)
            return resp
        except Exception as exp:
            print(exp)
            return HttpResponseServerError()

    return HttpResponseBadRequest()
