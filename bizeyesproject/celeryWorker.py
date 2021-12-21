from celery import Celery
from dbManager import DBManager
import datetime
import json
import uuid
import os

# rabbit
rabbitUsername = "test"
if "rabbitUsername" in os.environ:
    username = os.environ["rabbitUsername"]
rabbitPassword = "test"
if "rabbitPassword" in os.environ:
    rabbitPassword = os.environ["rabbitPassword"]
rabbitAddress = "192.168.3.100"
if "rabbitAddress" in os.environ:
    rabbitAddress = os.environ["rabbitAddress"]
# db
databaseName = "datamazraa"
if "databaseName" in os.environ:
    databaseName = os.environ["databaseName"]
databaseAddress = "192.168.3.91"
if "databaseAddress" in os.environ:
    databaseAddress = os.environ["databaseAddress"]
celeryName = "HELLO"
if "celeryName" in os.environ:
    celeryName = os.environ["celeryName"]

# create celery worker
# response as RPC
broker = 'amqp://{}:{}@{}//'.format(rabbitUsername,
                                    rabbitPassword, rabbitAddress)
app = Celery(celeryName, backend='rpc://', broker=broker)


# log events
logging = True

def WriteLog(Text="", Type=""):
    if logging:
        out = "[{}] - PBIQuery - {} - {}".format(
            datetime.datetime.now(), Text, Type)
        f = open("DataBaseLog.txt", "a")
        f.writelines(out)
        f.writelines("\n--------------------------------------\n")
        f.close()

@app.task
def iot_inner(task):
    db = DBManager(database=databaseName, server=databaseAddress)
    # input params
    ML_json = task["ml"]
    CL_json = task["cl"]
    date_json = task["tl"]
    type_result = task["cross_tab"].strip()
    output_type = task["retour"].strip()

    if ((not ML_json) or (type(ML_json) != list)):
        return {"error": "ML_json not defined/correct type"}
    if ((not CL_json) or (type(CL_json) != list)):
        return {"error": "CL_json not defined/correct type"}
    if ((not date_json) or (type(date_json) != list)):
        return {"error": "date_json not defined/correct type"}
    if ((not type_result) or (type(type_result) != str)):
        return {"error": "type_result not defined/correct type"}
    if ((not output_type) or (type(output_type) != str)):
        return {"error": "output_type not defined/correct type"}

    ML_json = json.dumps(task["ml"]).strip()
    CL_json = json.dumps(task["cl"]).strip()
    # name of cl and ml temp table
    date = datetime.datetime.now().strftime("%y%m%d%H%M%S")
    id = str(uuid.uuid4()).replace("-", "")
    ML_table_name = "ml_"+id+"_"+date
    CL_table_name = "cl_"+id+"_"+date
    # table name of pbi normalised
    table_name_pbi_normalised = "ubi_normalised_"+id+"_"+date
    # table name pbi crosstab
    table_name_pbi_crosstab = "ubi_crosstab_"+id+"_"+date

    # ****************create ml temp table with same name of Ml list in config file*****************************
    db.execProc("create_table_as_select", [ML_table_name, 'ml_temp'])
    WriteLog("62-- table {} created from pbi query ".format(ML_table_name), "INFO")

    # *********************insert data into Ml table created  ***************
    db.exec("insert into {} SELECT * FROM jsonb_populate_recordset(NULL::{}, '{}'::jsonb);".format(
        ML_table_name, ML_table_name, ML_json))
    WriteLog("58 insert data into {} ".format(ML_table_name), "INFO")

    # create cl temp table with same name of cl list in config file
    db.execProc("create_table_as_select", [CL_table_name, "cl_temp"])
    WriteLog("62 table {} crated ".format(CL_table_name), "INFO")

    # *********************insert data into cl table created  ***************
    query_insert_CL = "insert into {} SELECT * FROM jsonb_populate_recordset(NULL::{}, '{}'::jsonb);".format(
        CL_table_name, CL_table_name, CL_json)
    print(query_insert_CL)
    db.exec(query_insert_CL)
    WriteLog(" insert data into {} ".format(CL_table_name), "INFO")
    return iot_inner_work(db, date_json, type_result, output_type, ML_table_name, CL_table_name, table_name_pbi_normalised, table_name_pbi_crosstab)

@app.task
def powerBI(task):
    db = DBManager(database=databaseName, server=databaseAddress)
    # input params
    ML_json = json.dumps(task["ml"]).strip()
    CL_json = json.dumps(task["cl"]).strip()
    date_json = task["tl"]
    type_query = task["query"].strip()
    type_result = task["cross_tab"].strip()
    output_type = task["retour"].strip()
    # name of cl and ml temp table
    date = datetime.datetime.now().strftime("%y%m%d%H%M%S")
    id = str(uuid.uuid4()).replace("-", "")
    ML_table_name = "ml_"+id+"_"+date
    CL_table_name = "cl_"+id+"_"+date
    # table name of pbi normalised
    table_name_pbi_normalised = "ubi_normalised_"+id+"_"+date
    # table name pbi crosstab
    table_name_pbi_crosstab = "ubi_crosstab_"+id+"_"+date

    # ****************create ml temp table with same name of Ml list in config file*****************************
    db.execProc("create_table_as_select", [ML_table_name, 'ml_temp'])
    WriteLog("62-- table {} created from pbi query ".format(ML_table_name), "INFO")

    # *********************insert data into Ml table created  ***************
    db.exec("insert into {} SELECT * FROM jsonb_populate_recordset(NULL::{}, '{}'::jsonb);".format(
        ML_table_name, ML_table_name, ML_json))
    WriteLog("58 insert data into {} ".format(ML_table_name), "INFO")

    # create cl temp table with same name of cl list in config file
    db.execProc("create_table_as_select", [CL_table_name, "cl_temp"])
    WriteLog("62 table {} crated ".format(CL_table_name), "INFO")

    # *********************insert data into cl table created  ***************
    query_insert_CL = "insert into {} SELECT * FROM jsonb_populate_recordset(NULL::{}, '{}'::jsonb);".format(
        CL_table_name, CL_table_name, CL_json)
    db.exec(query_insert_CL)
    WriteLog(" insert data into {} ".format(CL_table_name), "INFO")

    if (type_query == "cluster"):
        return cluster_work(db, type_result, output_type, ML_table_name, CL_table_name, table_name_pbi_normalised, table_name_pbi_crosstab)
    elif (type_query == "iot_inner"):
        return iot_inner_work(db, date_json, type_result, output_type, ML_table_name, CL_table_name, table_name_pbi_normalised, table_name_pbi_crosstab)

def iot_inner_work(db, date_json, type_result, output_type, ML_table_name, CL_table_name, table_name_pbi_normalised, table_name_pbi_crosstab):
    output = "NULL"
    select_clause = ""
    limit_clause=""
    where_clause = ""
    for t in date_json:
        sqlc = t["SQLc"]
        if (sqlc == "where"):
            where_clause =t["SQL"]
        elif (sqlc == "select"):
            select_clause = t["SQL"]
        elif (sqlc == "limit"):
            limit_clause = t["SQL"]    

    if (type_result == "normalised"):
        if (output_type == "json"):

            if(select_clause == ""):
                # Call functio
                output = db.execFunc("pbi_inner_join_query_json", [
                                ML_table_name, CL_table_name, where_clause,limit_clause])
            else:
                output = db.execFunc("pbi_inner_join_query_json_select", [
                                ML_table_name, CL_table_name, where_clause, select_clause,limit_clause])
                return output

        elif (output_type == "table"):
            if(select_clause == ""):
                db.execProc("create_table_pbi_normalised_inner_join", [
                        table_name_pbi_normalised, ML_table_name, CL_table_name, where_clause])
                output = {"tablename": table_name_pbi_normalised}
            else:
                db.execProc("create_table_pbi_normalised_inner_join_select", [
                        table_name_pbi_normalised, ML_table_name, CL_table_name, where_clause,limit_clause])
                output = {"tablename": table_name_pbi_normalised}
        else:
            # no yet fonctionnelle
            output = {"result": "normalised but not json nor table"}
        # End normalised of inner_join
    else:
        # create table normalised
        # call procedure createTablePbiNormalised
        result_crosstab_table = "NULL"
        pbi_crosstab_table_query = "NULL"
        try:
            if(select_clause == ""):
                 # Call function
                db.execProc("create_table_pbi_normalised_inner_join", [
                        table_name_pbi_normalised, ML_table_name, CL_table_name,where_clause,limit_clause])
            else:
                r=db.execProc("create_table_pbi_normalised_inner_join_select", [
                        table_name_pbi_normalised, ML_table_name, CL_table_name, where_clause,select_clause,limit_clause])
                print("---------------------")
                print(r)
                print("---------------------")

            if(type_result == "cross_tab_ml"):
                pbi_crosstab_table_query = db.execFunc(
                    "inner_join_cross_tab_ml", [table_name_pbi_normalised])
                result_crosstab_table = db.exec(pbi_crosstab_table_query)
            # End if cross_tab_ml
            elif (type_result == "cross_tab_cl"):
                pbi_crosstab_table_query = db.execFunc(
                    "inner_join_cross_tab_cl", [table_name_pbi_normalised])
                result_crosstab_table = db.exec(pbi_crosstab_table_query)

        except Exception as exp:
            WriteLog("--------------------------", "Error")
            WriteLog("124", "Error")
            WriteLog(exp, "ERROR")
            WriteLog("--------------------------", "Error")
            return exceptionor(exp)
        if(output_type == "json"):
            outPut = db.exec('select array_to_json(array_agg(row_to_json(t)))from ({} ) AS t;'.format(
                pbi_crosstab_table_query))
            output = cleanup(outPut[0])
        elif(output_type == "table"):
            db.exec('create table {} As( {} )'.format(
                table_name_pbi_crosstab, pbi_crosstab_table_query))
            output = {"tablename": table_name_pbi_crosstab}
        else:
            output = result_crosstab_table
        # End else (not normalised)
    # drop table normalised
    db.execProc("drop_table", [table_name_pbi_normalised])
    # drop table normalised
    if (not((type_result == "normalised") and (output_type == "table"))):
        db.execProc("drop_table", [table_name_pbi_normalised])
    db.execProc("drop_table", [CL_table_name])
    db.execProc("drop_table", [ML_table_name])
    db.close()
    return output

def cluster_work(db, type_result, output_type, ML_table_name, CL_table_name, table_name_pbi_normalised, table_name_pbi_crosstab):
    output = "NULL"
    # Start normalised of cluster
    if(type_result == "normalised"):
        if(output_type == "json"):
            # Call function <get_objective_json> from db to select the query
            output = db.execFunc("get_objective_json", [
                                 ML_table_name, CL_table_name])
        elif (output_type == "table"):
            # call procedure createTablePbiNormalised
            db.execProc("create_table_pbi_normalised_cluster", [
                        table_name_pbi_normalised, ML_table_name, CL_table_name])
            output = {"tablename": table_name_pbi_normalised}
        else:
            # Call function <get_objective> from db to select the query
            output = db.execFunc(
                "get_objective", [ML_table_name, CL_table_name])
    # End normalised of cluster
    else:
        # create table normalised
        # call procedure createTablePbiNormalised
        db.execProc("create_table_pbi_normalised_cluster", [
                    table_name_pbi_normalised, ML_table_name, CL_table_name])
        result_crosstab_table = "NULL"
        pbi_crosstab_table_query = "NULL"
        try:
            if(type_result == "cross_tab_ml"):
                # cal pivotcode function witch return the dynamique query to create crosstable
                pbi_crosstab_table_query = db.execFunc(
                    "cluster_cross_tab_ml", [table_name_pbi_normalised])
                result_crosstab_table = db.exec(
                    pbi_crosstab_table_query)

                # End if cross_tab_ml
            elif(type_result == "cross_tab_cl"):
                # cal pivotcode function witch return the dynamique query to create crosstable
                pbi_crosstab_table_query = db.execFunc(
                    "cluster_cross_tab_cl", [table_name_pbi_normalised])
                result_crosstab_table = db.exec(
                    pbi_crosstab_table_query)
                result_crosstab_table = str(
                    result_crosstab_table).replace("\n", "")
                # End if cross_tab_cl
        except Exception as exp:
            WriteLog('183', "ERROR")
            WriteLog(exp, "ERROR")
            WriteLog("----------------", "ERROR")
            return exceptionor(exp)

        if(output_type == "json"):
            out = db.exec('select array_to_json(array_agg(row_to_json(t)))from ({} ) AS t;'.format(
                pbi_crosstab_table_query))
            if (len(out) == 1):
                out = out[0]
            # remove empty elems (None)
            output = cleanup(out)
        elif(output_type == "table"):
            db.exec('create table {} As( {} )'.format(
                table_name_pbi_crosstab, pbi_crosstab_table_query))
            output = {"tablename": table_name_pbi_crosstab}
        else:
            output = result_crosstab_table
        # End else (not normalised)
        # drop table normalised
    if (not((type_result == "normalised") and (output_type == "table"))):
        db.execProc("drop_table", [table_name_pbi_normalised])

    db.execProc("drop_table", [CL_table_name])
    db.execProc("drop_table", [ML_table_name])
    db.close()
    return output

def cleanup(obj):
    output = []
    for key in obj:
        newObj = {}
        for elem2 in key:
            if (key[elem2] != None):
                newObj[elem2] = key[elem2]
        output.append(newObj)
    return output

def exceptionor(exp):
    return {"error": True, "arg": exp.args}
