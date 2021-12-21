


from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest
from django.http.response import HttpResponseNotFound
from back_DB_bridge import Back_DB_bridge
import re
import json
from .common import *
from .loginMgr import jwtVerifyRequest
# from cAuth.jwt import jwtVerify,jwtCreate


def handleTl(time=""):
    return json.loads("""
    [{
        "SQL": " iot.date between now() - interval 'TEMPLATE' and now()",
        "SQLc": "where asc"
      }
      ,
      {
        "SQL": "time_bucket('TEMPLATE', iot.date) AS time,LOCF(max(iot.value))as valeur",
        "SQLc": "select"
    }
    ]
    """.replace("\n","").strip().replace("TEMPLATE",time))


def testRequest(request):
    
    tasker = initDBB()
    data = tasker.getMaxCode()
    tasker.close()
    
    return JsonResponse(data=data,safe=False)