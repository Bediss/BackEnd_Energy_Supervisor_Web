# api
import json
from jsonschema import validate
from projectapp.apis.common import *
from .cloneV5 import cloneReport_api
from .cloneSynoptic import cloneSynopticV2
from .loginMgr import jwtVerifyRequest
from .getTl import _getTl2,_getTl

def clone(request):
    if request.method == "POST":
        isValid = jwtVerifyRequest(request)
        if type(isValid) != dict:
            return isValid
        try:
            _data = json.loads(request.body)
            reportId = _data.get("IDs", [None])[0]
            tempIds = copy.deepcopy(_data["IDs"])
            _data.pop('IDs', None)
            # reportId = _data.get("R_IDs", [None])[0]
            # reportId = _data.get("S_IDs", [None])[0] if reportId is None else reportId
            db = initDB()
            isSynoptic = db.exec("""
            SELECT "Body"::jsonb ? 'object' FROM public."Reporting_V3" where "Report_Code"='{}';
            """.format(reportId))
            db.close()
            isSynoptic = isSynoptic[0]

            if isSynoptic is True:
                _data.update({"S_IDs": tempIds})
                schema = {
                    "type": "object",
                    "required": ["S_IDs", "data"],
                    "properties": {
                        "S_IDs": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "minLength": 1
                            },
                            "minItems": 1,
                        },
                        "DS_IDs": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1
                        },
                        "data": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "minProperties": 1,
                                "properties": {
                                    "ml": {
                                        "type": "object",
                                        "required": ["members", "tag"],
                                        "additionalProperties": False,
                                        "properties": {
                                            "tag": {"type": "string", "minLength": 1},
                                            "members": mlSchema
                                        }
                                    },
                                    "cl": {
                                        "type": "object",
                                        "required": ["members", "tag"],
                                        "properties": {
                                            "tag": {"type": "string", "minLength": 1},
                                            "members": clSchema
                                        },
                                    },
                                    "tlCluster": {
                                        "required": ["id"],
                                        "type": "object",
                                        "properties": {
                                            "tag": {"type": "string", "minLength": 1},
                                            "id": {"type": "string", "minLength": 1}}
                                    },
                                    # "tl": {
                                    #     "type": "object",
                                    #     "required": ["members", "selected", "tag"],
                                    #     "properties": {
                                    #         "selected": {
                                    #             "type": "integer",
                                    #             "minimum": 0
                                    #         },
                                    #         "tag": {"type": "string", "minLength": 1},
                                    #         "members": tlSchema
                                    #     },
                                    # },
                                    "params": {
                                        "type": "object",
                                        "required": ["zpm"],
                                        "properties": {
                                            "title": {"type": "string"},
                                            "zpm": {"type": "string"},
                                            "DDRC": {"type": "string"},
                                            "prev": {"type": "string"},
                                        }
                                    }
                                }
                            },
                            "minItems": 1,
                        },
                        "DS_IDs": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1
                        },
                        "preview": {"type": "boolean"}
                    }

                }
            else:
                _data.update({"R_IDs": tempIds})
                schema = {
                    "type": "object",
                    "required": ["R_IDs", "data"],
                    "properties": {
                        "R_IDs": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "minLength": 1
                            },
                            "minItems": 1,
                        },
                        "DR_IDs": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1
                        },
                        "data": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "minLength": 1,
                                "properties": {
                                    "ml": {
                                        "type": "object",
                                        "required": ["members"],
                                        "properties": {
                                            "tag": {"type": "string", "minLength": 1},
                                            "lCopy": {"type": "boolean"},
                                            "members": mlSchema
                                        }
                                    },
                                    "cl": {
                                        "type": "object",
                                        "required": ["members"],
                                        "properties": {
                                            "tag": {"type": "string", "minLength": 1},
                                            "lCopy": {"type": "boolean"},
                                            "members": clSchema
                                        }
                                    },
                                    "absTitles": {"type": "array", "items": {"type": ["string", 'null']}},
                                    "yAxisTitles": {"type": "array", "items": {"type": "array", "items": {"type": "string", "minLength": 1}}},
                                    "title": {"type": "string", "minLength": 5},
                                    "description": {"type": "string"},
                                    "tags": {"type": "string"},
                                    "tlCluster": tlClusterCloneSchema,
                                    "tlIot": tliotCloneSchema,
                                }
                            }
                        },
                        "preview": {"type": "boolean"}
                    }
                }
            validate(instance=_data, schema=schema)
        except Exception as exp:
            print(exp)
            return HttpResponseBadRequest()

        if isSynoptic is True:
            return cloneSynopticV2(request=request, newBody=_data)
        else:
            tlCluster=_data["data"][0].get("tlCluster",None)
            tlIot=_data["data"][0].get("tlIot",None)
            db=initDB()
            if tlCluster and not tlIot:
                tlCluster=tlCluster.get("id")
                tlIot=db.exec(
                        """
                        SELECT pair FROM public.tl where tl_id='{}'
                        """.format(tlCluster))
                if tlIot is not None:
                    tlIot=tlIot[0]
                    if tlIot is not None:
                        _data["data"][0]["tlIot"]={"id":tlIot}
            if not tlCluster and tlIot:
                tlIot=tlIot.get("id")
                tlCluster=db.exec(
                        """
                        SELECT pair FROM public.tl where tl_id='{}'
                        """.format(tlIot))
                
                if tlCluster is not None:
                    tlCluster=tlCluster[0]
                    if tlCluster is not None:
                        _data["data"][0]["tlCluster"]={"id":tlCluster}
            db.close()
            return cloneReport_api(request=request, newBody=_data)
    return HttpResponseBadRequest()


def cloneSynopticDev_V2(synoptic, synopticData, tasker, save, superClone=False):

    synopticBody = synoptic["Body"]
    Selected_Global = synoptic["Selected_Global"]

    from_zpm = synopticBody["object"].get("zpm", "")
    from_DDRC = synopticBody["object"].get("DDRC", "")
    from_prev = synopticBody["object"].get("prev", "")
    from_period = synopticBody["object"].get("period", "")
    from_title = synopticBody["object"]["configLayout"].get("title", "")

    ml = ""
    cl = ""
    tl = ""
    params = ""

    mlTag = ""
    clTag = ""
    tlTag = ""

    if "ml" in synopticData:
        ml = synopticData["ml"]["members"]

        mlTag = synopticData["ml"]["tag"]

    if "cl" in synopticData:

        cl = synopticData["cl"]["members"]
        clTag = synopticData["cl"]["tag"]

    if "tl" in synopticData:
        tl = synopticData["tl"]["members"]

        tlTag = synopticData["tl"]["tag"]
        tlSelected = synopticData["tl"]["selected"]

    if "params" in synopticData:
        params = synopticData["params"]

    ml = {"type": "ml", "synopticData": ml,
          "tag": mlTag} if type(ml) is list else None
    cl = {"type": "cl", "synopticData": cl,
          "tag": clTag} if type(cl) is list else None
    tl = {"type": "tl", "synopticData": tl, "tag": tlTag,
          "selected": tlSelected} if type(tl) is list else None
    params = {"type": "params", "synopticData": params} if type(
        params) is dict else None
    mlSG = next(
        item for item in Selected_Global if item["Dim"].lower() == "ml")
    mlVar = mlSG["Dim_type"].lower() == "var"
    # mlLabel= mlSG["Dim_label"] if "Dim_label" in mlSG else ""
    mlLabel = mlSG.get("Dim_label", "")

    mlClone = mlSG["Dim_Clone"] is True
    mlMembers = mlSG["Dim_Member"]

    clSG = next(
        item for item in Selected_Global if item["Dim"].lower() == "cl")
    clVar = clSG["Dim_type"].lower() == "var"
    # clLabel= clSG["Dim_label"] if "Dim_label" in clSG else ""
    clLabel = clSG.get("Dim_label", "")

    clClone = clSG["Dim_Clone"] is True
    clMembers = clSG["Dim_Member"]

    tlSG = next(
        item for item in Selected_Global if item["Dim"].lower() == "tl")
    tlVar = tlSG["Dim_type"].lower() == "var"
    tlLabel = tlSG["Dim_label"] if "Dim_label" in tlSG else ""
    tlLabel = tlSG.get("Dim_label", "")

    mlLabel = mlLabel if mlLabel != "*" else ""
    clLabel = clLabel if clLabel != "*" else ""
    tlLabel = tlLabel if tlLabel != "*" else ""

    tlClone = tlSG["Dim_Clone"] is True
    tlBucker = tlSG["Dim_Bucket"] is True if "Dim_Bucket" in tlSG else False
    tlMembers = tlSG["Dim_Member"]

    mlTag = mlLabel if mlTag == "" else mlTag
    clTag = clLabel if clTag == "" else clTag
    tlTag = tlLabel if tlTag == "" else tlTag

    mlTag = " {}".format(mlTag.strip())
    clTag = " {}".format(clTag.strip())
    tlTag = " {}".format(tlTag.strip())

    sgf = {
        "ml": {
            "var": mlVar,
            "clone": mlClone,
            "label": mlTag,
            "members": mlMembers,
            "others": mlSG
        },
        "params": {
            "zpm": from_zpm,
            "DDRC": from_DDRC,
            "period": from_period,
            "prev": from_prev,
            "title": from_title
        },
        "cl": {
            "var": clVar,
            "clone": clClone,
            "label": clTag,

            "members": clMembers,
            "others": clSG

        },
        "tl": {
            "var": tlVar,
            "clone": tlClone,
            "label": tlTag,
            "members": tlMembers,
            "bucket": tlBucker,
            "others": tlSG,
        }
    }

    Report_TableauName = synopticBody["Report_TableauName"].strip(
    ) if "Report_TableauName" in synopticBody else ""

    reportName = "Synoptique{}{}{}{}".format(
        Report_TableauName, clTag, mlTag, tlTag).strip()

    workLoadClone = list()
    for m in sgf:
        if m.lower() == "tl":
            continue

        for l in [ml, cl, tl, params]:
            if l is not None:
                memberType = l["type"] if "type" in l else ""
                if memberType == m:

                    globalDataElement = sgf[m]
                    memberData = l["synopticData"] if "synopticData" in l else list(
                    )

                    affix = "m_name" if memberType == "ml" else "Le_Compteur" if memberType == "cl" else "sql" if memberType == "sql" else "params"
                    if affix.lower() == "params":
                        for elem in memberData:
                            synopticBody["object"][elem] = memberData[elem]

                    elif affix.lower() == "sql":
                        # todo
                        between_from = next(
                            (item for item in globalDataElement["members"] if item["SQLc"].find("where") != -1), None)
                        between_to = next(
                            (item for item in memberData if item["SQLc"].find("where") != -1), None)

                        bucket_from = next(
                            (item for item in globalDataElement["members"] if item["SQLc"].find("select") != -1), None)
                        bucket_to = next(
                            (item for item in memberData if item["SQLc"].find("select") != -1), "")

                        if between_from is not None and between_to is not None:
                            workLoadClone.append(
                                {"from": between_from, "to": between_to})
                        if bucket_from is not None and bucket_to is not None:
                            workLoadClone.append(
                                {"from": bucket_from, "to": bucket_to})

                    else:
                        synopticBody["object"]["MasterObj_Data_Query"]["ml"] = memberData

    synopticBody["object"]["configLayout"]["title"] = reportName

    synopticBody = renameElements_dummy_s_V2(workLoadClone, synopticBody)

    synoptic["Body"] = synopticBody
    if save is True:
        if superClone:
            synoptic["Report_Name"] = reportName
        else:
            checkNameTask = {
                "Table_name": "Reporting_V3",
                "Header_list": "Report_Name",
                "Header_value": reportName,
                # "Column_select_liste": "Report_Code;Body;Auteur;Report_EnergyName;Report_EnergyCode;Selected_Global;Report_TableauName",
                "Column_select_liste": "Report_Name",
                "Column_condition_select_list": "dist",
                "Column_orderby_list": "asc",
            }
            ifreportExist = tasker.display(checkNameTask)
            ifreportExist = bool(ifreportExist[0])

            synoptic["Report_Name"] = "{} {}".format(reportName, time.strftime(
                "%a %d %b %Y %H:%M:%S")) if ifreportExist is True else reportName
        Selected_Global = globalDataHandler_s_V2(
            synopticBody, Selected_Global, "clone", sgf)
        return synoptic

    return synoptic.get("Body")


def cleanup_s_V2(label, _type):
    if type(label) is not str:
        raise Exception(
            "lable is not str"+str(type(label)))

    if _type == "ml":
        label = re.sub(' _', '_', label)
        label = re.sub('_S$', '', label)
        label = re.sub('_S-1$', '-1', label)
        label = re.sub('_J$', '', label)
        label = re.sub('_J-1$', '-1', label)
        label = re.sub('_M$', '', label)
        label = re.sub('_M-1$', '-1', label)
        label = re.sub('_A$', '', label)
        label = re.sub('_A-1$', '-1', label)
        # return label.replace('_S', "").replace('_J', "").replace('_M', "").replace('_A', '')

        return label
        # return label.replace('_S', "").replace('_J', "").replace('_M', "").replace('_A', '')
    return label


def globalDataHandler_s_V2(synopticBody, Selected_Global, _type, sgf=None):

    if _type == "clone":
        for elem in Selected_Global:
            if elem.get("Dim") is not None:
                if elem["Dim"].lower() == "ml":
                    elem["Dim_label"] = sgf["ml"]["label"].strip()
                    elem["Dim_Member"] = synopticBody["object"]["MasterObj_Data_Query"].get(
                        "ml", list())
                if elem["Dim"].lower() == "cl":
                    elem["Dim_label"] = sgf["cl"]["label"].strip()
                    elem["Dim_Member"] = synopticBody["object"]["MasterObj_Data_Query"].get(
                        "cl", list())
                if elem["Dim"].lower() == "tl":
                    elem["Dim_label"] = sgf["tl"]["label"].strip()
                    elem["Dim_Member"] = synopticBody["object"]["MasterObj_Data_Query"].get(
                        "tl", list())

    return Selected_Global


def renameElements_dummy_s_V2(task: list, report: dict = dict()):
    if type(task) is not list:
        return False
    _report = json.dumps(report.copy())
    for elem in task:
        if len(elem) == 2:
            _from = elem["from"]
            _to = elem["to"]
            if _from is not None and _to is not None:

                str_to = json.dumps(_to)
                _fromNameKey = ""
                _fromNameValue = ""
                _fromCodeKey = ""
                _fromCodeValue = ""

                _fromNameKey = "Le_Compteur" if "Le_Compteur" in _from else "m_name" if "m_name" in _from else "SQL" if "SQL" in _from else "params"
                if _fromNameKey != "":
                    # _fromNameValue = _from[_fromNameKey] if type(_from) is not str else _from
                    _fromNameValue = _from[_fromNameKey]

                _fromCodeKey = "Code_Compteur" if "Code_Compteur" in _from else "m_code" if "m_code" in _from else "SQLc" if "SQLc" in _from else "params"
                if _fromCodeKey != "":
                    _fromCodeValue = _from[_fromCodeKey]
                if _fromCodeKey != "params":
                    dumpedFrom1 = json.dumps(
                        {_fromNameKey: _fromNameValue, _fromCodeKey: _fromCodeValue})
                    dumpedFrom2 = json.dumps(
                        {_fromCodeKey: _fromCodeValue, _fromNameKey: _fromNameValue})
                else:
                    dumpedFrom1 = _fromNameValue
                    dumpedFrom2 = _fromNameValue
                    cleanParams = _to["params"].replace('"', '')
                    str_to = cleanParams

                _report = _report.replace(
                    dumpedFrom1, str_to).replace(dumpedFrom2, str_to)

                _toName = _to["Le_Compteur"] if "Le_Compteur" in _to else _to["m_name"] if "m_name" in _to else ""

                if _fromNameValue != _toName and _fromCodeKey != "SQL" and _fromCodeKey != "SQLc":
                    _report = _report.replace('"{}"'.format(
                        _fromNameValue), '"{}"'.format(_toName))
    return json.loads(_report)
