import os
from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest,HttpResponseNotFound,HttpResponseServerError,FileResponse

from back_worker_bridge import Back_worker_bridge
from back_DB_bridge import Back_DB_bridge
import json
import copy
import re
import time
from jsonschema import validate


databasename = os.environ["databasename"] if "databasename" in os.environ else "vmzdb3_29_09_2021"
databaseServer = os.environ["databaseServer"] if "databaseServer" in os.environ else "localhost"
databaseUser = os.environ["databaseUser"] if "databaseUser" in os.environ else "postgres"
databasePassword = os.environ["databasePassword"] if "databasePassword" in os.environ else "root"
databasePort = int(os.environ["databasePort"]) if "databasePort" in os.environ else 5432

pdfServer = os.environ["pdfServer"] if "pdfServer" in os.environ else "http://192.168.3.100"
pdfGeneratorPort = int(os.environ["pdfGeneratorPort"]) if "pdfGeneratorPort" in os.environ else 3001

rabbitServer = os.environ["rabbitServer"] if "rabbitServer" in os.environ else '192.168.3.100'
rabbitUserName = os.environ["rabbitUserName"] if "rabbitUserName" in os.environ else 'test'
rabbitPassword = os.environ["rabbitPassword"] if "rabbitPassword" in os.environ else 'test'

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

