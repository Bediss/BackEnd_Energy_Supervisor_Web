
from projectapp.apis.common import *
from django.http.response import HttpResponse
import jwt
import json
from datetime import datetime,timedelta


def custom_jwt_middleware(get_response):
    # One-time configuration and initialization.

    def middleware(request):
        if request.method!="OPTION":
            # token=request.headers.get("token",None)
            # if token == superToken:
            #     print("superToken used")
            # else:
            #     ""
            print(request.method)
        # Code to be executed for each request before
        # the view (and later middleware) are called.

        response = get_response(request)

        # Code to be executed for each request/response after
        # the view is called.

        return response

    return middleware