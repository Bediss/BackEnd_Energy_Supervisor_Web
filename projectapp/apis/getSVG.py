
from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getSVG(request):
    isValid=jwtVerifyRequest(request)
    if type(isValid) !=dict:
                return isValid
    if request.method == "GET":
        id = request.GET.get("id", None)
        name = request.GET.get("name", None)
        res= _getSVG(id=id,name=name)
        if res == 400:
            return HttpResponseBadRequest()
        elif res==404:
            return HttpResponseNotFound()
        else:
            return JsonResponse(res,safe=False)
    return HttpResponseBadRequest()

def _getSVG(id=None,name=None):
    db = initDB()
                        
    if name is not None:
        resp=db.exec("select row_to_json(t)from ( SELECT * FROM public.synoptic_svg where name=%s) t",(name))
    elif id is not None:
        resp=db.exec("select row_to_json(t)from ( SELECT * FROM public.synoptic_svg where id=%s) t",(id,))
    else:
        resp=db.exec("select array_to_json(array_agg(row_to_json(t))) from ( SELECT * FROM public.synoptic_svg) t")

    resp = 404 if resp is None else resp[0]
    return resp


