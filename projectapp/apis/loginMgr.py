
from projectapp.apis.common import *
import jwt
from datetime import datetime,timedelta

SECRET=os.environ["SECRET"] if "SECRET" in os.environ else "hindi"


def jwtVerify(token):
    try:
        if token == superToken:
            return {"code":200,"msg":"ok"}
        decoded = jwt.decode(token, SECRET,algorithm="HS256")
        exp=decoded.get("exp")
        iat=decoded.get("iat")
        now=int(datetime.timestamp(datetime.now(tz=TIME_ZONE)))

        if exp<now:
            return {"code":401,"msg":"token expired"}
        return {"code":200,"msg":"ok","user":decoded}
    except:
        return {"code":401,"msg":"invalid token"}

def jwtVerifyRequest(request):
    res=HttpResponse()
    res["Access-Control-Allow-Origin"] = "*"
    # res["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    try:
        token=request.headers.get("token",None)
        r=jwtVerify(token)
        if r["code"]==200:
            return r
        else:
            res.status_code=r["code"]
    except:
        res.status_code=401
    return res

def jwtCreate(args):
    email=args.get("email",None)
    password=args.get("password",None)
    if not email or not password:
        return {"code":400,"msg":"username and password must be present"}
    users=getUsers()
    user=next((user for user in users if user["Email_User_Master"]==email and user["Password"]==password),None)
    if user is None:
        return {"code":404,"msg":"invalid user"}
    _user={
        "email":user["Email_User_Master"],
        "userId":user["User_Master_Code"],
        "exp":datetime.now(tz=TIME_ZONE)+timedelta(hours=8),
        # "exp":datetime.now(tz=TIME_ZONE)+timedelta(seconds=10),
        "iat":datetime.now(tz=TIME_ZONE)
        }
    token= jwt.encode(_user, SECRET,algorithm="HS256").decode("utf-8")
    return {"code":200,"token":token}
    

def login(request):
    if request.method=="POST":
        try:
            schema={
                "type": "object",
                "required": ["email", "password"],
                "properties": {
                    "email": {
                        "type":"string",
                        "minLength":1
                    },
                    "password": {
                        "type":"string",
                        "minLength":1
                    },
                }
            }
            data=json.loads(request.body)
            validate(instance=data,schema=schema)
            email=data["email"].lower()
            password=data["password"]
            resp=jwtCreate({"email":email,"password":password})
            status=resp["code"]
            data=dict()
            if status == 200:
                data={"token":resp["token"]}
            else:
                data={"msg":resp["msg"]}
            res= JsonResponse(data=data)
            res.status_code=resp["code"]
            return res
        except:
            HttpResponseBadRequest()
    return HttpResponseBadRequest()
