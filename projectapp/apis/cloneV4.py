
import json
from jsonschema import validate
from django.http.response import HttpResponseNotFound, HttpResponseServerError
from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest
import copy
import re
import time
from .common import *


def cloneReport_dev4(request):
    if request.method == 'POST' or request.method == 'PUT':

        try:
            _data = json.loads(request.body)

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
                                    },
                                },
                                "cl": {
                                    "type": "object",
                                    "required": ["members"],
                                    "properties": {
                                        "tag": {"type": "string", "minLength": 1},
                                        "lCopy": {"type": "boolean"},
                                        "members": clSchema
                                    },
                                },
                                "tl": {
                                    "type": "object",
                                    "required": ["members", "selected"],
                                    "properties": {
                                        "selected": {
                                            "type": "integer",
                                            "minimum": 0
                                        },
                                        "tag": {"type": "string", "minLength": 1},
                                        "members": tlSchema
                                    },
                                },
                            },

                        }
                    },
                    "preview": {"type": "boolean"}
                }
            }

            validate(instance=_data, schema=schema)
        except Exception as exp:
            print("cloneReport_dev2")
            print(exp)
            return (HttpResponseBadRequest())

        if len(_data["R_IDs"]) != len(_data["data"]):
            return HttpResponseBadRequest()

        R_IDs = _data.get("R_IDs")
        DR_IDs = _data.get("DR_IDs")
        superClone = False
        if DR_IDs is not None:
            if len(DR_IDs) != len(R_IDs):
                return HttpResponseBadRequest()
            else:
                superClone = True

        tasker = Back_DB_bridge(database=databasename, username=databaseUser,
                                password=databasePassword, server=databaseServer, port=databasePort)

        data = _data["data"] if "data" in _data else None
        save = False
        preview = False

        clonedReports = None
        if request.method == 'PUT':
            save = True
            bad = False
            for elem in _data.get("data", list()):
                if elem.get("ml") is not None:
                    if elem["ml"].get("tag", False) is False:
                        bad = True
                if elem.get("cl") is not None:
                    if elem["cl"].get("tag", False) is False:
                        bad = True
                if elem.get("tl") is not None:
                    if elem["tl"].get("tag", False) is False:
                        bad = True
            if bad is True:
                return HttpResponseBadRequest()
            preview = bool(_data["preview"]) if "preview" in _data else False
        else:
            for elem in _data.get("data", list()):
                if elem.get("ml") is not None:
                    if elem["ml"].get("tag", False) is False:
                        elem["ml"]["tag"] = ",".join(
                            map(lambda r: r["m_name"], elem["ml"]["members"]))

                if elem.get("cl") is not None:
                    if elem["cl"].get("tag", False) is False:
                        elem["cl"]["tag"] = ",".join(
                            map(lambda r: r["Le_Compteur"], elem["cl"]["members"]))

        clonedReports = cloneReportV4(
            R_IDs=R_IDs, data=data, tasker=tasker, save=save, superClone=superClone)
        tasker.closeConnection()

        if save is True:
            tasker = Back_DB_bridge(database=databasename, transaction=True)

            # fix tl query & prep for save
            def clonedReportsCountFilter(clonedReport):
                return True if clonedReport is not None else False

            clonedReportsCount = len(
                list(filter(clonedReportsCountFilter, clonedReports)))

            task = {"Table_name": "Reporting_V3",
                    "nbr_code": clonedReportsCount}

            if DR_IDs is not None:
                maxCodes = DR_IDs
            else:
                maxCodes = tasker.getMaxCode(task)
                maxCodes = maxCodes.replace(
                    "[", "").replace("]", "").split(",")

            if maxCodes is None or maxCodes == "[]":
                return (HttpResponseServerError())

            maxCodesBack = copy.deepcopy(maxCodes)

            for ii in range(len(clonedReports)):
                if clonedReports[ii] is None:
                    continue
                report = copy.deepcopy(clonedReports[ii])
                report["Report_Code"] = maxCodes.pop(0)
                report["DBAction"] = 1
                for i in range(len(report["Body"]["objects"])):
                    rep = report["Body"]["objects"][i]

                    xS = rep["MasterObj_Data_selection"]["masterObjectX"]
                    tS = rep["MasterObj_Data_selection"]["MasterObjPage"]["membersList"]
                    for j in range(len(xS)):
                        if "SQL" in xS[j]:
                            if "''" not in xS[j]["SQL"]:
                                fixedTl = xS[j]["SQL"].replace("'", "''")

                                report["Body"]["objects"][i]["MasterObj_Data_selection"]["masterObjectX"][j]["SQL"] = fixedTl

                    for j in range(len(tS)):
                        if "SQL" in tS[j]:
                            if "''" not in tS[j]["SQL"]:
                                fixedTl = tS[j]["SQL"].replace("'", "''")

                                report["Body"]["objects"][i]["MasterObj_Data_selection"]["masterObjectX"][j]["SQL"] = fixedTl

                report["Selected_Global"] = json.loads(json.dumps(report["Selected_Global"]).replace(
                    "'", "''")) if "Selected_Global" in report else dict()
                clonedReports[ii] = report.copy()

            task = {
                "Table_name": "Reporting_V3",
                "data": clonedReports
            }
            # save
            res = tasker.update(task)
            # res={"op":"ok"}

            if "op" in res:
                if res["op"] == "ok":
                    if preview is False:
                        res["IDs"] = maxCodesBack
                        return HttpResponse(json.dumps(res), content_type="application/json")
                else:
                    return HttpResponseServerError()
            else:
                return HttpResponseServerError()
        clonedReportsOut = list()
        for rep in clonedReports:
            clonedReportsOut.append(
                rep["Body"] if rep is not None and "Body" in rep else None)

        return HttpResponse(json.dumps(clonedReportsOut), content_type="application/json")

    return (HttpResponseBadRequest())


def cleanupV4(label, _type):
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


def cloneReportV4(R_IDs: list, data: list, tasker, save=False, superClone=False):

    task = {
        "Table_name": "Reporting_V3",
        "Header_list": "Report_Code",
        "Header_value": ",".join(R_IDs),
        # "Column_select_liste": "Report_Code;Body;Auteur;Report_EnergyName;Report_EnergyCode;Selected_Global;Report_TableauName",
        "Column_select_liste": "",
        "Column_condition_select_list": "dist",
        "Column_orderby_list": "asc",
    }

    response = tasker.display(task)
    response = response[0]
    output = list()
    for report_index in range(len(R_IDs)):

        __report = next(
            (item for item in response if item["Report_Code"] == R_IDs[report_index]), None)

        report = copy.deepcopy(__report) if type(__report) is dict else None
        Selected_Global = report["Selected_Global"] if type(
            report) is dict and "Selected_Global" in report else None

        if report is None or Selected_Global is None:
            output.append(None)
            continue

        reportData = data[report_index]

        ml = ""
        cl = ""
        tl = ""
        lCopyML = False
        lCopyCL = False
        lCopyTL = False
        mlTag = ""
        clTag = ""

        tlTag = ""
        if "ml" in reportData:
            ml = reportData["ml"]["members"]
            if "lCopy" in reportData["ml"]:
                lCopyML = reportData["ml"]["lCopy"]
            mlTag = reportData["ml"]["tag"]

        if "cl" in reportData:

            cl = reportData["cl"]["members"]
            if "lCopy" in reportData["cl"]:
                lCopyCL = reportData["cl"]["lCopy"]
            clTag = reportData["cl"]["tag"]

        if "tl" in reportData:
            tl = reportData["tl"]["members"]
            if "lCopy" in reportData["tl"]:
                lCopyTL = reportData["tl"]["lCopy"]

            tlTag = reportData["tl"]["tag"]
            tlSelected = reportData["tl"]["selected"]

        ml = {"type": "ml", "reportData": ml, "lCopy": lCopyML,
              "tag": mlTag} if type(ml) is list else None
        cl = {"type": "cl", "reportData": cl, "lCopy": lCopyCL,
              "tag": clTag} if type(cl) is list else None
        tl = {"type": "tl", "reportData": tl, "lCopy": lCopyTL, "tag": tlTag,
              "selected": tlSelected} if type(tl) is list or type(tl) is str else  None
        mlSG = next(
            item for item in Selected_Global if item["Dim"].lower() == "ml")
        mlVar = mlSG["Dim_type"].lower() == "var"
        mlLabel = mlSG["Dim_label"] if "Dim_label" in mlSG else ""

        mlClone = mlSG["Dim_Clone"] is True
        mlMembers = mlSG["Dim_Member"]

        clSG = next(
            item for item in Selected_Global if item["Dim"].lower() == "cl")
        clVar = clSG["Dim_type"].lower() == "var"
        clLabel = clSG["Dim_label"] if "Dim_label" in clSG else ""

        clClone = clSG["Dim_Clone"] is True
        clMembers = clSG["Dim_Member"]

        tlSG = next(
            item for item in Selected_Global if item["Dim"].lower() == "tl")
        tlVar = tlSG["Dim_type"].lower() == "var"
        tlLabel = tlSG["Dim_label"] if "Dim_label" in tlSG else ""
        tlClone = tlSG["Dim_Clone"] is True
        tlBucker = tlSG["Dim_Bucket"] is True if "Dim_Bucket" in tlSG else False
        tlMembers = tlSG["Dim_Member"]

        mlTag = mlLabel if mlTag == "" else mlTag
        clTag = clLabel if clTag == "" else clTag
        tlTag = tlLabel if tlTag == "" else tlTag
        mlTag = " "+mlTag
        clTag = " "+clTag
        tlTag = " "+tlTag

        sgf = {
            "ml": {
                "var": mlVar,
                "clone": mlClone,
                "label": mlTag,
                "members": mlMembers,
                "others": mlSG
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

        Report_TableauName = report["Report_TableauName"].strip(
        ) if "Report_TableauName" in report else ""

        reportName = "{}{}{}{}".format(
            Report_TableauName, clTag, mlTag, tlTag).strip()
        # reportName = "{}{}{}{}".format(Report_TableauName," "+clTag if clTag !="" else ""," "+mlTag if mlTag !="" else ""," "+tlTag if tlTag !="" else "").strip()

        body = report["Body"] if "Body" in report else dict()

        for singleObject in body["objects"]:
            if singleObject["MasterObj_Data_selection"]["x"].lower() == "tl":
                singleObject["MasterObj_Data_Mapping"]["xaxis"]["title"]["text"] = tlTag.strip()

        if len(body) > 0 and "configLayout" in body:
            body["configLayout"]["title"] = reportName

        workLoadClone = list()
        workLoadCopy = list()

        for m in sgf:

            for l in [ml, cl, tl]:
                if l is not None:
                    memberType = l["type"] if "type" in l else ""
                    if memberType == m:
                        globalDataElement = sgf[m]
                        memberData = l["reportData"] if "reportData" in l else list(
                        )
                        if globalDataElement["clone"] is True:
                            affix = "m_name" if memberType == "ml" else "Le_Compteur" if memberType == "cl" else "SQL"
                            if affix.lower() == "sql":
                                if type(memberData) == list:
                                    between_from = next(
                                        (item for item in globalDataElement["members"] if item["SQLc"].find("where") != -1), None)
                                    between_to = next(
                                        (item for item in memberData if item["SQLc"].find("where") != -1), None)

                                    bucket_from = next(
                                        (item for item in globalDataElement["members"] if item["SQLc"].find("select") != -1), None)
                                    bucket_to = next(
                                        (item for item in memberData if item["SQLc"].find("select") != -1), "")

                                    if between_from is not None and between_to is not None:
                                        workLoadClone.append({"from": between_from, "to": between_to})
                                    if bucket_from is not None and bucket_to is not None:
                                        workLoadClone.append({"from": bucket_from, "to": bucket_to})
                                else:
                                    tl_from=globalDataElement["members"]
                                    tl_to=memberData
                                    if tl_from is not None and tl_to is not None:
                                        workLoadClone.append({"from": tl_from, "to": tl_to})         


                            else:
                                for member in memberData:
                                    ___to = member
                                    ___to[affix] = cleanupV4(
                                        member[affix], memberType)
                                    ___from = next(
                                        (item for item in globalDataElement["members"] if item[affix] == ___to[affix]), None)
                                    if ___from is not None and ___to is not None:
                                        workLoadClone.append(
                                            {"from": ___from, "to": ___to})

                        elif globalDataElement["var"] is True:

                            if l["lCopy"]:

                                for sri in range(len(report["Body"]["objects"])):
                                    sr = report["Body"]["objects"][sri]
                                    x = sr["MasterObj_Data_selection"]["x"].lower()
                                    y = sr["MasterObj_Data_selection"]["y"].lower()
                                    isPage = sr["MasterObj_Data_selection"]["page"]["type"].lower(
                                    ) != "a"
                                    page = sr["MasterObj_Data_selection"]["page"]["page"] if isPage else None

                                    target = x if memberType == x else y if memberType == y else page if memberType == page else None

                                    report["Body"]["objects"][sri]["MasterObj_Data_selection"][target] = memberData
                            else:
                                max = len(memberData) if len(memberData) > len(
                                    globalDataElement["members"]) else len(globalDataElement["members"])
                                for i in range(max):

                                    ___from = globalDataElement["members"][i] if len(
                                        globalDataElement["members"]) > i else None
                                    ___to = memberData[i] if len(
                                        memberData) > i else None

                                    if ___from is not None and ___to is not None:
                                        if type(___from) is dict and type(___to) is dict:
                                            if None not in list(___from.values()) and None not in list(___to.values()):
                                                workLoadCopy.append(
                                                    {"from": ___from, "to": ___to})

        reportWithReplacedElements = renameElements_dummyV4(
            workLoadClone, report)
        reportWithReplacedElements = renameElements_dummyV4(
            workLoadCopy, reportWithReplacedElements)

        if save is True:

            Selected_GlobalWithReplacedElements = globalDataHandlerV4(
                workLoadCopy, Selected_Global, "copy")

            Selected_GlobalWithReplacedElements = globalDataHandlerV4(workLoadClone, Selected_GlobalWithReplacedElements, "clone", {
                "ml": ml, "cl": cl, "tl": tl})
            reportWithReplacedElements["Selected_Global"] = Selected_GlobalWithReplacedElements
            # Report_TableauName," "+clTag," "+mlTag, " "+tlTag if tlTag !="" else "" ).strip()
            if superClone is True:
                reportWithReplacedElements["Report_Name"] = reportName
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

                if ifreportExist is False:
                    reportWithReplacedElements["Report_Name"] = reportName
                else:
                    reportWithReplacedElements["Report_Name"] = "{} {}".format(
                        reportName, time.strftime("%a %d %b %Y %H:%M:%S"))

        output.append(reportWithReplacedElements)

    return output


def globalDataHandlerV4(workLoad, _globalData, _type, dim=None):
    globalData = _globalData.copy()
    if _type == "clone":

        for i in range(len(globalData)):
            elem = globalData[i]
            dumped = json.dumps(elem["Dim_Member"])
            globalData[i]["Dim_Clone"] = False
            for wl in workLoad:
                _from = wl["from"]
                mct = "ml" if "m_name" in _from else "cl" if "Le_Compteur" in _from else "tl"

                if mct.lower() == "tl":
                    ""
                    _to = wl["to"]
                    dumped = dumped.replace(_from, _to)
                else:
                    firstKey = list(_from.keys())[0]
                    firstValue = cleanupV4(_from[firstKey], mct)

                    secondkey = list(_from.keys())[1]
                    secondValue = cleanupV4(_from[secondkey], mct)

                    _from1 = json.dumps(
                        {firstKey: firstValue, secondkey: secondValue})
                    _from2 = json.dumps(
                        {secondkey: secondValue, firstKey: firstValue})
                    _to = json.dumps(wl["to"])

                    dumped = dumped.replace(_from1, _to)
                    dumped = dumped.replace(_from2, _to)
                target = "m_name" if "m_name" in wl["from"] else "Le_Compteur"
                if target in wl["from"]:
                    dumped = dumped.replace(
                        wl["from"][target], wl["to"][target])
            globalData[i]["Dim_Member"] = json.loads(dumped)
    else:
        ""

    if dim is not None:
        if globalData[i]["Dim"].lower() == "ml" and dim["ml"] is not None:
            globalData[i]["Dim_label"] = dim["ml"]["tag"]
        if globalData[i]["Dim"].lower() == "cl" and dim["cl"] is not None:
            globalData[i]["Dim_label"] = dim["cl"]["tag"]
        if globalData[i]["Dim"].lower() == "tl" and dim["tl"] is not None:
            globalData[i]["Dim_label"] = dim["tl"]["tag"]
    return globalData


def renameElements_dummyV4(task: list, report: dict = dict()):

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
                if type(_from) is list:
                    _fromNameKey = "Le_Compteur" if "Le_Compteur" in _from else "m_name" if "m_name" in _from else "SQL"
                    if _fromNameKey != "":
                        print("_from",_from)
                        print("_fromNameKey",_fromNameKey)
                        _fromNameValue = _from[_fromNameKey]

                    _fromCodeKey = "Code_Compteur" if "Code_Compteur" in _from else "m_code" if "m_code" in _from else "SQLc"
                    if _fromCodeKey != "":
                        _fromCodeValue = _from[_fromCodeKey]



                    dumpedFrom1 = json.dumps(
                        {_fromNameKey: _fromNameValue, _fromCodeKey: _fromCodeValue})
                    dumpedFrom2 = json.dumps(
                        {_fromCodeKey: _fromCodeValue, _fromNameKey: _fromNameValue})
                    _report = _report.replace(
                        dumpedFrom1, str_to).replace(dumpedFrom2, str_to)
                elif type(_from) is str:
                    _report = _report.replace(_from, _to)
                _toName = _to["Le_Compteur"] if "Le_Compteur" in _to else _to["m_name"] if "m_name" in _to else ""

                if _fromNameValue != _toName and _fromCodeKey != "SQL" and _fromCodeKey != "SQLc":
                    _report = _report.replace('"{}"'.format(
                        _fromNameValue), '"{}"'.format(_toName))

    return json.loads(_report)

