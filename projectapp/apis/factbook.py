

from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getFactBooks(request):
    isValid=jwtVerifyRequest(request)
    if type(isValid) !=dict:
                return isValid
    if request.method == 'GET':
        db=initDB()
        res=db.exec("""
        select array_to_json(array_agg(row_to_json(t))) from ( SELECT "Code_FactBook","Nom_FactBook","Factbook_Membre" from public."FactBook_V3") t
        """)
        db.close()
        res=res[0]
        return JsonResponse(data=res,safe=False)
    return HttpResponseBadRequest()

def getFactBookById(request):

    if request.method == 'GET':

        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                    return isValid

        factBookIds=request.GET.get("factBookId",None)

        if factBookIds is None:
            return HttpResponseBadRequest()
        db=initDB()
        _factBookIds=prepQueryParams(factBookIds.split(","))
        res=db.exec("""
        select row_to_json(t)from (SELECT "Code_FactBook","Nom_FactBook","Factbook_Membre" from public."FactBook_V3" where "Code_FactBook" in ({})) t
        """.format(_factBookIds["txt"]),_factBookIds["tuple"])
        db.close()
        _res=list()
        for id in _factBookIds["tuple"]:
            fb=dict()
            if res is not None:
                fb=next((_fb for _fb in res if _fb and _fb["Code_FactBook"]==id),None)
            _res.append(fb if fb else dict())
        return JsonResponse(data=_res,safe=False)

        
    return HttpResponseBadRequest()

