
from .common import *

def getSVG(request):
    if request.method == "GET":
        id = request.GET.get("id", None)
        name = request.GET.get("name", None)
        res= _getSVG(id=id,name=name)
        if res == 400:
            return HttpResponseBadRequest()
        elif res==404:
            return HttpResponseNotFound()
        else:
            return HttpResponse(json.dumps(res),content_type='application/json')
    return HttpResponseBadRequest()

def _getSVG(id=None,name=None):
    db = Back_DB_bridge(database=databasename, username=databaseUser,
                        password=databasePassword, server=databaseServer, port=databasePort)
                        
    if name is not None:
        resp=db.db.exec("select row_to_json(t)from ( SELECT * FROM public.synoptic_svg where name='{}') t".format(name))
    elif id is not None:
        resp=db.db.exec("select row_to_json(t)from ( SELECT * FROM public.synoptic_svg where id='{}') t".format(id))
    else:
        resp=db.db.exec("select array_to_json(array_agg(row_to_json(t))) from ( SELECT * FROM public.synoptic_svg) t")

    resp = 404 if resp is None else resp[0]
    return resp


