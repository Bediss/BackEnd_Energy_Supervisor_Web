from re import findall
from dbManager import DBManager
import uuid
import json
import datetime
import copy

class Back_DB_bridge(object):
    """
    link backend to database
    transaction:
        one time use open new connection on starting new transaction to rollback
    ontimeUse:
        self close/disconnect after use (1time)
    logging:
        default disabled
    printLog:
        default True:if logging enabled print to screen

    available functions:
        display
        update
        cluster
        getMaxCode
        complexQuery beta
        operationStatus : get db status
        closeConnection : close connection to db
    """
    def __init__(self, database="postgres", username="postgres", password="root", server="localhost", port=5432, ontimeUse=True, transaction=False, logging=False, printLog=True,dictCursor=False):
        if not database:
            raise Exception("database is required")
        if not username:
            raise Exception("username is required")
        if not password:
            raise Exception("password is required")
        if not type(port) == int and port > 0:
            raise Exception("port must be int")
        if not type(ontimeUse) == bool:
            raise Exception("port must be boolean")
        self.ontimeUse = ontimeUse
        self.transaction = transaction
        self.logging = logging
        self.__dictCursor=dictCursor
        self.printLog = printLog
        self.db = DBManager(database=database, username=username, password=password,
                            server=server, port=port, autoCommit=not transaction,dictCursor=self.__dictCursor)

    def __WriteLog(self, Text="", Type=""):
        if self.logging:
            if self.printLog:
                print(
                    "[{}] - PBIQuery - {} - {}".format(datetime.datetime.now(), Text, Type))
            else:
                out = "[{}] - PBIQuery - {} - {}".format(
                    datetime.datetime.now(), Text, Type)
                f = open("DataBaseLog.txt", "a",encoding="UTF-8")
                f.writelines(out)
                f.writelines("\n--------------------------------------\n")
                f.close()

    def display(self, task):
        try:
            Table_name = task["Table_name"]
            Header_list = task["Header_list"]
            Header_value = task["Header_value"]
        except Exception as exp:
            return {"error": exp.args}

        Column_select_liste = None
        Column_condition_select_list = 'DISTINCT'
        Column_orderby_list = 'desc'

        if "Column_select_liste" in task:
            Column_select_liste = task["Column_select_liste"]
        if "Column_condition_select_list" in task:
                Column_condition_select_list = task["Column_condition_select_list"]
        if "Column_orderby_list" in task:
            Column_orderby_list = task["Column_orderby_list"]

        json_query = ""
        display_query = ""
        Select_query = ""
        where_query = ""
        orderby_clause = ""
        orderby_condition_array = ""
        if Column_select_liste:
            # tester si exist un orderby clause
            if Column_orderby_list:
                # ligne from file config
                orderby_condition_array = Column_orderby_list.split(";")

            select_column_array = Column_select_liste.split(";")
            if len(select_column_array) == 1 and select_column_array[0] == "":
                select_column_array = []
            condition_column_array = Column_condition_select_list.split(";")
            if len(condition_column_array) == 1 and condition_column_array[0] == "":
                condition_column_array = []
            nbr_column = len(select_column_array)

            list_distinct_column = []
            list_non_distinct_column = []
            list_column_to_orderby = []
            condition_order = ""
            clause_select_distinct = ""

            column_order = ""
            column_distinct = ""
            column_non_distinct = ""

            distinct_clause = ""
            for i in range(len(orderby_condition_array)):
                if (orderby_condition_array[i] != "*"):
                    condition_order = orderby_condition_array[i]
            for i in range(nbr_column):

                if (len(condition_column_array) >= i+1 and condition_column_array[i] != "*"):
                    list_distinct_column.append(select_column_array[i])
                else:
                    list_non_distinct_column.append(select_column_array[i])
                    if orderby_condition_array:
                        if (orderby_condition_array != "*"):
                            # cad colum a ajouter dans la list order by
                            list_column_to_orderby.append(
                                select_column_array[i])
            # End for

            # *****************************************************************************
            if list_column_to_orderby:
                for i in range(len(list_column_to_orderby)):
                    column_order = column_order + '"' + \
                        list_column_to_orderby[i] + '"' + ","

                # eliminer le dernier virgule from column_order string
                # contien list des colonnes à ordonner qui ne sont pas dans la list distinct colonnes
                column_order = self.__removeLastComma(column_order)

            # ****************************************************************************
            if list_distinct_column:
                # parcourir list distnct column
                for list_distinct_column_elem in list_distinct_column:
                    column_distinct = column_distinct + '"' + list_distinct_column_elem + '"' + ","

                # eliminer le dernier virgule from column_distinct string
                column_distinct = column_distinct[:len(column_distinct) - 1]

                # construire clause distict
                distinct_clause = "DISTINCT ON (" + \
                                                column_distinct + ") " + column_distinct
            # end if disctict list not empty

            if list_non_distinct_column:
                # parcourir list non distnct column
                for list_non_distinct_column_elem in list_non_distinct_column:
                    column_non_distinct = column_non_distinct + \
                        '"{}",'.format(list_non_distinct_column_elem)
                # eliminer le dernier virgule from column_non_distinct string

                column_non_distinct = column_non_distinct[:len(
                    column_non_distinct) - 1]
            # end if non_disctict list not empty
            #
            if list_non_distinct_column and list_distinct_column:
                clause_select_distinct = "select " + distinct_clause + " , " + column_non_distinct
            else:
                clause_select_distinct = "select " + distinct_clause + column_non_distinct

            # ***********construire order clause************************************

            if list_non_distinct_column and column_order:
                orderby1 = ""
                orderby2 = ""
                if orderby_clause or column_distinct:
                    orderby1 = orderby_clause + column_distinct
                if column_order or condition_order:
                    orderby2 = column_order+" " + \
                        condition_order if column_order and condition_order else column_order + condition_order

                orderby_clause = orderby1 + "," + \
                    orderby2 if orderby1 and orderby2 else orderby1 if orderby1 else orderby2
                orderby_clause = " ORDER BY " + orderby_clause
                # orderby_clause = " ORDER BY " + orderby1 + "," + orderby2
            else:
                orderby_clause = " ORDER BY " + orderby_clause + \
                    column_distinct + column_order + " " + condition_order
            Select_query = clause_select_distinct + " from " + '"' + Table_name + '"'
        else:
            Select_query = "select * from " + '"' + Table_name + '"'
        # end if exist select list
        # ******************End of clause select************

        if Header_list != "*":
            # if#2 display with filter

            QUERY_HEADER = Header_list.split(";")
            QUERY_VALUE = Header_value.split(";")
            MAX_Fields = len(QUERY_VALUE)
            MAX_HEADER = len(QUERY_HEADER)
          
            for i in range(MAX_HEADER):
                # list of values for each header
                list_value_field = QUERY_VALUE[i].split(",")
                # number of values for each headerr
                value_number = len(list_value_field)
                value_field = ""
                if (value_number > 1):
                    for j in range(value_number):
                        value_field = value_field + "'" + \
                            list_value_field[j] + "'" + ","
                    value_field = value_field + "'" + \
                        list_value_field[value_number - 1] + "'"
                    where_query = where_query + '"' + \
                        QUERY_HEADER[i] + '"' + \
                            " in ( " + value_field + " ) " + " and "
                
                # end if list value of field supp 1
                else:
                    if QUERY_VALUE[i] == '%':
                        where_query = where_query + '"' + \
                            QUERY_HEADER[i] + '"' + " like " + \
                                "'" + QUERY_VALUE[i] + "'" + " and "
                    else:
                        where_query = where_query + '"' + \
                            QUERY_HEADER[i] + '"' + " = " + \
                                "'" + QUERY_VALUE[i] + "'" + " and "
            # end for
            if where_query.endswith(" and "):
                    index=where_query.rindex(" and ")
                    where_query=where_query[0:index]
            
            # list of values for last header
            list_value_field = QUERY_VALUE[MAX_Fields - 1].split(",")
            # number of values for last headerr
            value_number = len(list_value_field)
            value_field = ""
            display_query = Select_query + " where " + where_query + orderby_clause

        # End if#2
        else:
            # display all without filter
            display_query = Select_query
            # Write-Host -ForegroundColor Green "Exporting Data from Table_name in treatment....... "
        self.__WriteLog(
            "Exporting Data from {} table in treatment.......".format(Table_name), "INFO")

        try:
            json_query = 'select array_to_json(array_agg("{}"))from ({}) "{}"'.format(
                Table_name, display_query, Table_name)
                
            self.__WriteLog(json_query, "QUERY")
            return self.db.exec(json_query)
        except Exception as exp:
            self.__WriteLog(exp.args, "ERROR")
            return self.__exceptionor(exp)

    def __removeLastComma(self, string):
        if len(string) > 0:
            if (string[-1] == ","):
                string = string[:-1]
        return string

    def update_old(self, task):
        if (not self.transaction):
            raise Exception("DBManager not in transaction mode")
        tableName = str(task["Table_name"]).strip()
        data = json.dumps(task["data"])
        temp_table = tableName+"_modify"
        # <<<<*******************start check rows to add, update and delete***************************************>>>>>
        try:
            # vider le table  ***************
            self.db.execProc("truncate_table", [temp_table])
            self.__WriteLog("{} truncated".format(temp_table), "INFO")
            # copy data from json  to temptable ***************
            self.db.execProc("insert_from_json", [temp_table, data])
            # call the delete_proc to delete data
            self.db.execProc("delete_proc", [tableName, temp_table])
            # call the add_proc to insert data
            self.db.execProc("add_proc", [tableName, temp_table])
            self.__WriteLog(
                "insert data into {} has been successfull ".format(tableName), "INFO")
            # call update_proc to update data
            self.db.execProc("update_proc", [tableName, temp_table])
            self.__WriteLog(
                "update data into {} has been successfull ".format(tableName), "INFO")
            self.db.commit()
            self.db.close()
            return {"op": "ok"}
        except Exception as inst:
            self.__WriteLog(inst.args, "INFO")
            return self.__exceptionor(inst)

    def update(self, task):
        if (not self.transaction):
            raise Exception("DBManager not in transaction mode")
        tableName = str(task["Table_name"]).strip()
        data = json.dumps(task["data"])
        modify_table = tableName+"_modify"

        date = datetime.datetime.now().strftime("%y%m%d%H%M%S")
        id = str(uuid.uuid4()).replace("-", "")
        temp_table = modify_table+"_"+id+"_"+date
        # <<<<*******************start check rows to add, update and delete***************************************>>>>>
        try:
            self.db.startTransaction()

            result=self.db.addTransaction("truncate_table", [modify_table])

            result=self.db.addTransaction("create_table_as_select", [temp_table, modify_table])
            if (result["error"] is True) : raise Exception({"error":True,"args":result["args"]})
            result=self.db.addTransaction("insert_from_json", [temp_table, data])
            if (result["error"] is True) : raise Exception({"error":True,"args":result["args"]})
            result=self.db.addTransaction("delete_proc", [tableName, temp_table])
            if (result["error"] is True) : raise Exception({"error":True,"args":result["args"]})
            result=self.db.addTransaction("add_proc", [tableName, temp_table])
            if (result["error"] is True) : raise Exception({"error":True,"args":result["args"]})
            result=self.db.addTransaction("update_proc", [tableName, temp_table])
            if (result["error"] is True) : raise Exception({"error":True,"args":result["args"]})
            result=self.db.addTransaction("drop_table", [temp_table])
            if (result["error"] is True) : raise Exception({"error":True,"args":result["args"]})
            self.db.commitTransaction()
            return {"op": "ok"}
        except Exception as inst:
            self.__WriteLog(inst.args, "INFO")
            self.db.rollbackTransaction()
            return self.__exceptionor(inst)
        finally:
            self.db.close()

    def getMaxCode_old(self, task):
        try:
            Table_name = task["Table_name"]
            nbr_code = task["nbr_code"]

            # *** query to get max code and prefixe from tale tableoftable****
            getmaxcode_query = "select max_code , prefixe_code from tableoftable where name = '{}'".format(
                Table_name)
            result = self.db.exec(getmaxcode_query)
            if result:

                if type(result) == tuple and len(result) == 1:
                    self.__WriteLog(result, "ERROR")
                    return {"error": result}
                max_code = result[0]
                prefixe_code = result[1]
                listcode = []
                for code in range(int(nbr_code)):
                    listcode.append("{}{}".format(prefixe_code, max_code+code))
                # ******incremante max_code******
                incremante_query = "UPDATE tableoftable set max_code = max_code+{} WHERE name = '{}'".format(
                    nbr_code, Table_name)
                self.db.exec(incremante_query, resultExist=False)
                self.__WriteLog(
                    "returning max_code={}  has been successfull".format(max_code), "INFO")
                return listcode
            else:
                return "no data"
        except Exception as exp:
            self.__exceptionor(exp)

    def getMaxCode(self, task):
        # "get_max_code_list('Email',2)"
        try:
            Table_name = task["Table_name"]
            nbr_code = task["nbr_code"]
            return self.db.execFunc("get_max_code_list", [Table_name, nbr_code])
        except Exception as exp:
            self.__exceptionor(exp)

    def complexQuery(self, task):

        master_query_code = task["master_query_code"]
        output_type = task["output_type"]
        master_tablename = "AllCompteur"  # table to select from after join

        t1 = task["t1"]
        t2 = task["t2"]
        t3 = task["t3"]
        t4 = task["t4"]
        
        header_t1 = task["header_t1"]
        header_t2 = task["header_t2"]
        header_t3 = task["header_t3"]
        header_t4 = task["header_t4"]

        value_t1 = task["value_t1"]
        value_t2 = task["value_t2"]
        value_t3 = task["value_t3"]
        value_t4 = task["value_t4"]

        Table_name_array = []
        Header_list_array = []
        Header_value_array = []

        global_where_query = " where "
        # *******parcourir file jusqu a la fin*******

        # list des table for query in (exmpl: Allcompteur,pcc)
        if t1 is not None and len(t1) > 0:
            Table_name_array.append(t1)
        if t1 is not None and len(t1) > 0:
            Table_name_array.append(t2)
        if t1 is not None and len(t1) > 0:
            Table_name_array.append(t3)
        if t1 is not None and len(t1) > 0:
            Table_name_array.append(t4)

        # header list to query in for each table
        if header_t1 is not None and len(t1) > 0:
            Header_list_array.append(header_t1)
        if header_t1 is not None and len(header_t2) > 0:
            Header_list_array.append(header_t2)
        if header_t1 is not None and len(header_t3) > 0:
            Header_list_array.append(header_t3)
        if header_t1 is not None and len(header_t4) > 0:
            Header_list_array.append(header_t4)

        # header value for each header
        if header_t1 is not None and len(value_t1) > 0:
            Header_value_array.append(value_t1)
        if header_t1 is not None and len(value_t2) > 0:
            Header_value_array.append(value_t2)
        if header_t1 is not None and len(value_t3) > 0:
            Header_value_array.append(value_t3)
        if header_t1 is not None and len(value_t4) > 0:
            Header_value_array.append(value_t4)

        for i in range(len(Table_name_array)):
                Table_name = Table_name_array[i]
                Header_list = Header_list_array[i]
                Header_value = Header_value_array[i]
                # call function wherequery to concatenate the query
                where_query = self.__wherequery(Table_name, Header_list, Header_value)
                global_where_query = global_where_query + where_query + " and "
            # End for

        #********concatenate the last value off arry Table_name_array***************************************************************
        Table_name = Table_name_array[-1]
        Header_list = Header_list_array[-1]
        Header_value = Header_value_array[-1]
        where_query = self.__wherequery(Table_name,Header_list,Header_value)
        global_where_query = global_where_query + where_query
        #********* End concatenate the last value off arry Table_name_array*********************************************************
        #select la partie initial de query from table*******************************************************************************
        get_select_query = "SELECT query FROM master_query WHERE CODE= '{}'".format(master_query_code)
        result=self.db.exec(get_select_query)
        # select_query = result.query
        return result 
        #**************************************************************************************************************************
        final_query = select_query + " " + global_where_query

        #Write-Host -ForegroundColor Magenta "final_query==" final_query

        if(output_type == "csv"):
                #Export the result query in csv
                result_final_query=self.db.exec(final_query)
                #*********************Export the result final query in csv**********************************************************
                # result_final_query  | ConvertTo-CSV -Delimiter ';' -NoTypeInformation | ForEach-Object { _ -Replace '"', ""} | 
                # Out-File OutPut_File_csv  -fo -en ascii
        elif (output_type == "json"):
                    json_query='select array_to_json(array_agg("' + master_tablename +'"))from ('+final_query+ ') "'+master_tablename +'"'
                    json_result=self.db.exec(json_query)
                    return json_result

    def __wherequery(self,Table_name, Header_list, Header_value):

        QUERY_HEADER = Header_list.split(";")
        QUERY_VALUE = Header_value.split(";")
        MAX_Fields = len(QUERY_HEADER)

        for i in range(MAX_Fields):
            # list of values for each header
            list_value_field = QUERY_VALUE[i].split(",")
            # number of values for each headerr
            value_number = len(list_value_field)
            value_field = ""
            where_query = ""
            if value_number > 1:
                for j in range(value_number):
                    value_field = value_field + "'" + \
                        list_value_field[j] + "'" + ","
                # value_field of last index in array
                value_field = value_field + "'" + \
                    list_value_field[value_number - 1] + "'"
                where_query = where_query + '"' + Table_name + '"' + "." + '"' + \
                    QUERY_HEADER[i] + '"' + \
                        " in ( " + value_field + " ) " + " and "

            # end if list value of field supp 1
            else:
                if (QUERY_VALUE[i] == '%'):
                    where_query = where_query + '"' + Table_name + '"' + "." + '"' + \
                        QUERY_HEADER[i] + '"' + " like " + \
                            "'" + QUERY_VALUE[i] + "'" + " and "

                else:
                    where_query = where_query + '"' + Table_name + '"' + "." + '"' + \
                        QUERY_HEADER[i] + '"' + " = " + "'" + \
                            QUERY_VALUE[i] + "'" + " and "

        # end for

        # list of values for last header******************************************************************************
        list_value_field = QUERY_VALUE[MAX_Fields - 1].split(",")
        # number of values for last headerr
        value_number = len(list_value_field)
        value_field = ""
        if (len(QUERY_VALUE[MAX_Fields - 1].split(",")) > 1):
            for j in range(value_number):
                value_field = value_field + "'" + list_value_field[j] + "'" + ","

            value_field = value_field + "'" + \
                list_value_field[value_number - 1] + "'"
            where_query = where_query + '"' + Table_name + '"' + "." + '"' + \
                QUERY_HEADER[MAX_Fields - 1] + '"' + " in ( " + value_field + " ) "
        # end if list value of field supp 1
        else:
            if (QUERY_VALUE[MAX_Fields - 1] == '%'):
                where_query = where_query + '"' + Table_name + '"' + "." + '"' + \
                    QUERY_HEADER[MAX_Fields - 1] + '"' + " like " + \
                        "'" + QUERY_VALUE[MAX_Fields - 1] + "'"

            else:
                where_query = where_query + '"' + Table_name + '"' + "." + '"' + \
                    QUERY_HEADER[MAX_Fields - 1] + '"' + " = " + \
                        "'" + QUERY_VALUE[MAX_Fields - 1] + "'"

        return where_query

    def operationStatus(self):
        return self.db.OPstatus()

    def closeConnection(self):
        return self.db.close()

    def cluster(self, task):

        # input params
        ML_json = json.dumps(task["ml"]).strip()
        CL_json = json.dumps(task["cl"]).strip()
        TL = task.get("tl",None)
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
        self.db.execProc("create_temp_table_as_select", [ML_table_name, 'ml_temp'])

        # *********************insert data into Ml table created  ***************
        self.db.exec("insert into {} SELECT * FROM jsonb_populate_recordset(NULL::{}, '{}'::jsonb);".format(
            ML_table_name, ML_table_name, ML_json))
        # create cl temp table with same name of cl list in config file
        self.db.execProc("create_temp_table_as_select", [CL_table_name, "cl_temp"])

        # *********************insert data into cl table created  ***************
        query_insert_CL = "insert into {} SELECT * FROM jsonb_populate_recordset(NULL::{}, '{}'::jsonb);".format(
            CL_table_name, CL_table_name, CL_json)
        self.db.exec(query_insert_CL)
        return self.__cluster_work(self.db, type_result, output_type, ML_table_name, CL_table_name, table_name_pbi_normalised, table_name_pbi_crosstab,TL,ML_json,CL_json)

    def __cluster_work(self, db, type_result, output_type, ML_table_name, CL_table_name, table_name_pbi_normalised, table_name_pbi_crosstab,TL,ML_json,CL_json):
        output="NULL"
        error=False
        # Start normalised of cluster
        if(type_result == "normalised"):
            if(output_type == "json"):
                # Call function <get_objective_json> from db to select the query
                if TL is None:
                    output=db.execFunc("get_objective_json", [
                        ML_table_name, CL_table_name])
                else:
                    where=next((elem for elem in TL if elem["SQLc"].find("where")!=-1),dict()).get("SQL",400)
                    limit=next((elem for elem in TL if elem["SQLc"].find("limit")!=-1),dict()).get("SQL",400)
            
                    if where == 400 or limit == 400:
                        return 400
                    db.execProc("create_table_pbi_normalised_cluster_iot", [
                                table_name_pbi_normalised, ML_table_name, CL_table_name, where, limit])
                    output=db.exec("""
                    select array_to_json(array_agg(row_to_json(t)))from (select * from {} ) AS t;
                    """.format(table_name_pbi_normalised))
                    output=output if output is not None else list([None])
                    output=output[0]
                    _____output=list()
                    
                    if type(output) is list:
                        for cl in json.loads(CL_json):
                            for ml in json.loads(ML_json):
                                _cc_name=cl["Le_Compteur"]
                                _cc=cl["Code_Compteur"]
                                _m=ml["m_code"].split("_")[0]
                                cc_m="{},{}".format(_cc,_m)
                                _elem=next((item for item in output if item["Le_Compteur"]==_cc_name and item["m_name"]==ml['m_name']),dict())
                                elem=copy.deepcopy(_elem)
                                if len(elem) != 0:
                                    __temp=elem.get('datalive',None)
                                    elem.pop("datalive",None)
                                    elem.update({"value":__temp})
                                    elem.update({"cc_m":cc_m})
                                    _____output.append(elem)
                        output=_____output
            elif (output_type == "table"):
                # call procedure createTablePbiNormalised
                db.execProc("create_table_pbi_normalised_cluster", [
                            table_name_pbi_normalised, ML_table_name, CL_table_name])
                output={"tablename": table_name_pbi_normalised}
        else:
            if TL is None:
                db.execProc("create_table_pbi_normalised_cluster", [
                            table_name_pbi_normalised, ML_table_name, CL_table_name])
            else:
                
                where=next((elem for elem in TL if elem["SQLc"].find("where")!=-1),dict()).get("SQL",400)
                limit=next((elem for elem in TL if elem["SQLc"].find("limit")!=-1),dict()).get("SQL",400)
         
                if where == 400 or limit == 400:
                    return 400
                db.execProc("create_table_pbi_normalised_cluster_iot", [
                            table_name_pbi_normalised, ML_table_name, CL_table_name, where, limit])
          
            result_crosstab_table="NULL"
            pbi_crosstab_table_query="NULL"
            try:
                if(type_result == "cross_tab_ml"):
                    # cal pivotcode function witch return the dynamique query to create crosstable
                    pbi_crosstab_table_query=db.execFunc(
                        "cluster_cross_tab_ml", [table_name_pbi_normalised,ML_table_name])
                    result_crosstab_table=db.exec(
                        pbi_crosstab_table_query)
                    # End if cross_tab_ml
                elif(type_result == "cross_tab_cl"):
                    # cal pivotcode function witch return the dynamique query to create crosstable
                    pbi_crosstab_table_query=db.execFunc(
                        "cluster_cross_tab_cl", [table_name_pbi_normalised,CL_table_name])
                    result_crosstab_table=db.exec(
                        pbi_crosstab_table_query)
                    result_crosstab_table=str(
                        result_crosstab_table).replace("\n", "")
                    # End if cross_tab_cl
            except Exception as exp:
                error=True

            if not error:
                if(output_type == "json"):
 
                    out=db.exec('select array_to_json(array_agg(row_to_json(t)))from ({} ) AS t;'.format(
                        pbi_crosstab_table_query))

                    out=out if type(out) is tuple else tuple((None,))
                    if (len(out) == 1):
                        output=out[0]
                    else:
                        output=out
                elif(output_type == "table"):
                    db.exec('create table {} As( {} )'.format(
                        table_name_pbi_crosstab, pbi_crosstab_table_query))
                    output={"tablename": table_name_pbi_crosstab}
                else:
                    output=result_crosstab_table
                # End else (not normalised)
                # drop table normalised
        if (not((type_result == "normalised") and (output_type == "table"))):
            db.execProc("drop_table", [table_name_pbi_normalised])

        # db.execProc("drop_table", [CL_table_name])
        # db.execProc("drop_table", [ML_table_name])
        if (self.ontimeUse):
            db.close()
        return output

    def __cleanup(obj):
        output=[]
        for key in obj:
            newObj={}
            for elem2 in key:
                if (key[elem2] != None):
                    newObj[elem2]=key[elem2]
            output.append(newObj)
        return output

    def __exceptionor(self, exp):
        self.__WriteLog(str(exp), "ERROR")

        return exp.args
    def login(self,info,secret,expire=3600):
        username=info["username"]
        password=info["password"]
        resp=self.db.exec("")
        ok=False
        if not ok:
            return None
        else:
            resp=self.db.exec()
            token_content_uncoded = {
                "username": {}.get('username'),
                "password": {}.get('password'),
                "email": {}.get('email')
                }
            refresh_Token=jwt.encode(token_content_uncoded,secret)
            temp = refresh_Token.get('refreshToken')
            actual_refresh_token = temp.decode("utf-8")
    
            ts = int(time.time()) # adding issual_time and expire_time
            access_token_content = {
                "username": token_content_uncoded.get('username'),
                "password": token_content_uncoded.get('password'),
                "email": token_content_uncoded.get('email'),
                "issual_time": ts,
                "expire_time" : ts+expire
            }
            jwt_token = {'token': jwt.encode(access_token_content,secret)}
            u = jwt_token.get('token')
            actual_access_token = u.decode("utf-8")
            ts = float(time.time())

            final_payload_x = {"user":
                    {
                        "userName": token_content_uncoded.get('username'),
                        "email": token_content_uncoded.get('email'),
                        "issual_time": int(ts),
                        "expire_time" : int(ts+expire)
                    },

                        "jwtToken": actual_access_token,
                        "refreshToken" : actual_refresh_token
                    }

        if self.ontimeUse:
            self.closeConnection()
        return final_payload_x
       