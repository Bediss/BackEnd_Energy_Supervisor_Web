
from django.http.response import HttpResponseForbidden
from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest
import datetime


def inputValuePMG(request):
    if request.method == "PUT":
        try:
            isValid = jwtVerifyRequest(request)
            if type(isValid) != dict:
                return isValid
            userType=isValid.get("user",dict()).get("userType",None)
            token=request.headers.get("token",None)
            if "saisie" not in userType.split("|") or superToken==token:
                return HttpResponseForbidden()
            body = json.loads(request.body)
            schema = {
                "type": "object",
                "required": ["date","poids"],
                "properties": {
                    "date": {
                            "type": "string",
                            # "format": "date",
                            "pattern": "^[1-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]$"
                             },
                    "poids": {"type": "number"}
                }
            }
            # DDMMYYYY
            validate(instance=body, schema=schema)

            _load = body.get("poids")
            _date = body.get("date")

            _dateParsed = datetime.datetime.strptime(_date, "%Y-%m-%d").date().strftime("%d%m%Y")
            db=initDB()
            res=db.execFunc("scrape_mfg_graisse",[_load,_dateParsed])
            q=db.execProc("scrape_data_mfg_id_iot",[res,0])
            db.close()

            respStatus=200
            if q.get("error",False) is True:
                respStatus=500
            resp = JsonResponse(data=body)
            resp.status_code=respStatus
            return resp
        except:
            return HttpResponseBadRequest()
    return HttpResponseBadRequest()
