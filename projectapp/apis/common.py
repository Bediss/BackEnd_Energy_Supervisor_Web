import os
from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest, HttpResponseNotFound, HttpResponseServerError, FileResponse

from back_worker_bridge import Back_worker_bridge
from back_DB_bridge import Back_DB_bridge
from dbManager import DBManager
import json
import copy
import re
import time
from jsonschema import validate
from pytz import timezone


preps = dict({
    "energies": list(),
    "EnergyTable":list(),
    "levels": list(),
    "types": list(),
    "mls": list(),
    "allEnergies": list(),
    "allCompteurs":list(),
    "EnergyMeasureNormalised":list(),
    "tl":list()
})

users = list()

def initPrep():
    global preps
    db = initDB()
    try:
        tl=db.exec("""
        select array_to_json(array_agg(row_to_json(t)))from (select * from "tl") AS t;
        """)
        preps["tl"] = tl[0] if type(tl[0]) is list else list()

        EnergyMeasureNormalised = db.exec("""
        select array_to_json(array_agg(row_to_json(t)))from (select * from "EnergyMeasureNormalised") AS t;
        """)

        preps["EnergyMeasureNormalised"] = EnergyMeasureNormalised[0] if type(EnergyMeasureNormalised[0]) is list else list()

        allCompteursTable = db.exec("""
        select array_to_json(array_agg(row_to_json(t)))from (select * from "AllCompteur") AS t;
        """)

        preps["allCompteurs"] = allCompteursTable[0] if type(allCompteursTable[0]) is list else list()
        preps["energies"] = list(set([item["Energie"] for item in preps["allCompteurs"] if item["Energie"]]))

        energyTable = db.exec("""
        select array_to_json(array_agg(row_to_json(t)))from (select * from "Energy") AS t;
        """)
        preps["EnergyTable"] = energyTable[0] if type(energyTable[0]) is list else list()

        print("connected")
    except:
        print("not connected")
    finally:
        db.close()

eventsOn=False

databasename = os.environ["databasename"] if "databasename" in os.environ else "vmz_29_11_2021_Test"
databaseServer = os.environ["databaseServer"] if "databaseServer" in os.environ else "192.168.3.91"
databaseUser = os.environ["databaseUser"] if "databaseUser" in os.environ else "postgres"
databasePassword = os.environ["databasePassword"] if "databasePassword" in os.environ else "root"
databasePort = int(os.environ["databasePort"]) if "databasePort" in os.environ else 5432

pdfServer = os.environ["pdfServer"] if "pdfServer" in os.environ else "192.168.3.100"
pdfGeneratorPort = int(os.environ["pdfGeneratorPort"]) if "pdfGeneratorPort" in os.environ else 3001

rabbitServer = os.environ["rabbitServer"] if "rabbitServer" in os.environ else '192.168.3.100'
rabbitUserName = os.environ["rabbitUserName"] if "rabbitUserName" in os.environ else 'test'
rabbitPassword = os.environ["rabbitPassword"] if "rabbitPassword" in os.environ else 'test'

superToken = os.environ["superToken"] if "superToken" in os.environ else 'superToken'
TIME_ZONE = timezone(
    os.environ["tz"]) if "tz" in os.environ else timezone('Africa/Tunis')

clSchema = {
    "type": 'array',
    "items": {
        "type": "object",
        "required": ["Le_Compteur", "Code_Compteur"],
        "properties": {
            "Le_Compteur": {"type": "string"},
            "Code_Compteur": {"type": "string"},
        }
    },
    "minItems": 1,
    "uniqueItems": True,
}

mlSchema = {
    "type": 'array',
    "items": {
        "type": "object",
        "required": ["m_code", "m_name"],
        "properties": {
            "m_code": {"type": "string"},
            "m_name": {"type": "string"},
        }
    },
    "minItems": 1,
    "uniqueItems": True,
}
tlSchema = {
    "anyOf": [
        {
            "type": "array",

            "items": {
                "type": "object",
                "required": ["SQL", "SQLc"],
                "properties": {
                    "SQL": {"type": "string"},
                    "SQLc": {"type": "string"},
                }
            },
            "minItems": 2,
            "maxItems": 2,
            "uniqueItems": True,

        },
        {
            "type": "string",
            "minLength": 4
        }
    ]
}

tliotCloneSchema = {
    "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "string"},
                "tag": {"type": "string"},
                "selected": {
                    "type": "integer",
                    "minimum": 0
                }
            }
}

tlClusterCloneSchema = {
    "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "string"},
                "tag": {"type": "string"}
            }
}

tlCloneSchema_old = {
    "type": "array",
            "items": {
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "tag": {"type": "string"},
                    "selected": {
                        "type": "integer",
                        "minimum": 0
                    }
                }
            },
    "minItems": 1,
    # "maxItems": 2,
    # "uniqueItems": True,
}


def initDB(autoCommit=True, dictCursor=False,autoConnect=True):
    return DBManager(database=databasename, username=databaseUser, password=databasePassword,
                     server=databaseServer, port=databasePort, autoCommit=autoCommit, dictCursor=dictCursor,autoConnect=autoConnect)


def initDBB(dictCursor=False, transaction=False):
    return Back_DB_bridge(database=databasename, username=databaseUser, password=databasePassword, server=databaseServer, port=databasePort, transaction=transaction, dictCursor=dictCursor)

def initUsers():
    global users
    db = initDB()
    users = db.exec("""
        select array_to_json(array_agg(row_to_json(t))) from ( SELECT * FROM public."User_Master") t
        """)
    users = users[0]
    db.close()

def getUsers():
    return users

def prepQueryParams(input=list) -> dict():
    out = list()
    outTuple = tuple()
    for i in input:
        out.append("%s")
        outTuple = outTuple+(i,)
    out = ",".join(out)
    return {
        "tuple": outTuple,
        "txt": out
    }

def listeners():
    if eventsOn is True:
        def callback(params=None, payload=None):
    
                _type=params.get("type")
                payload = json.loads(payload)
                action = payload.get("op")
                data = json.loads(payload["data"])
                print("------------------")
                print(_type)
                print(data)
                print("------------------")
                if action.lower() == 'update':
                    if _type=="users":
                        i = next((i for i in range(len(users)) if users[i].get("User_Master_Code", None) == data["User_Master_Code"]), None)
                        if i is not None:
                            users[i] = data
                    elif _type == 'tl':
                        i = next((i for i in range(len(preps["tl"])) if preps["tl"][i].get("tl_id", None) == data["tl_id"]), None)
                        if i is not None:
                            preps["tl"][i] = data
                    elif _type == 'allCompteurs':
                        print(data)
                        i = next((i for i in range(len(preps["allCompteurs"])) if preps["allCompteurs"][i].get("Code_Compteur", None) == data["Code_Compteur"]), None)
                        if i is not None:
                            preps["allCompteurs"][i] = data

                            print(preps["allCompteurs"][i])
                elif action.lower() == 'create':
                    if _type=="users":
                        users.append(data)
                    elif _type == 'tl':
                        preps["tl"].append(data)
                    elif _type == 'allCompteurs':
                        preps["allCompteurs"].append(data)
                elif action.lower() == 'delete':
                    if _type=="users":
                        i = next((i for i in range(len(users)) if users[i].get("User_Master_Code", None) == data["User_Master_Code"]), None)
                        users.pop(i)
                    elif _type == 'tl':
                        i = next((i for i in range(len(preps["tl"])) if preps["tl"][i].get("tl_id", None) == data["tl_id"]), None)
                        preps["tl"].pop(i)
                    elif _type == 'allCompteurs':
                        i = next((i for i in range(len(preps["allCompteurs"])) if preps["allCompteurs"][i].get("Code_Compteur", None) == data["Code_Compteur"]), None)
                        preps["allCompteurs"].pop(i)
        dbuser = initDB()
        dbuser.listenV2("user_notif",callback=callback,params={"type":"users"},separateThread=True)
        dbtl = initDB()
        dbtl.listenV2("tl_notif",callback=callback,params={"type":"tl"},separateThread=True)
        dbac = initDB()
        dbac.listenV2("allCompteurs_notif",callback=callback,params={"type":"allCompteurs"},separateThread=True)