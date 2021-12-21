

from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest

def getEnergies(request):
    if request.method == "GET":
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                return isValid
        energies=preps.get("energies",None)
        resp=JsonResponse(data=energies,safe=False)
        return resp
    return HttpResponseBadRequest()

def getAllEnergies(request):
    if request.method == "GET":
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                    return isValid
        data=preps["EnergyTable"]
        resp=JsonResponse(data=data,safe=False)
        return resp
    return HttpResponseBadRequest()
