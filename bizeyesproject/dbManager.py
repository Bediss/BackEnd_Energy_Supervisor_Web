import psycopg2
from psycopg2 import extras

class DBManager(object):
    """
        simple db interface
        available functions:
            OPstatus    : db status
            closed      : check if connection to db is closed
            commit      : commit if transaction is on
            close       : close connection to db
            info        : get connected db name
            execFunc    : exec function:
                function name :str
                params  : list
            execProc    : exec procedure
                procedure name :str
                params : list
            exec        : exec query
            startTransaction    : start transaction
            addTransaction      : add transaction to list
            commitTransaction   : commit previously added transactions
            execTrans           : exec transactions list then commit
                                    params exp [{"proc":[param1,param2]}]

    """
    def __init__(self, database, username="postgres", password="root", server="localhost", port=5432, autoCommit=True,dictCursor=False):
        if not database:
            raise Exception("database is required")
        if not username:
            raise Exception("username is required")
        if not password:
            raise Exception("password is required")
        if not server:
            raise Exception("server is required")
        if not type(port) == int and port > 0:
            raise Exception("port must be int")
        self.conn = None
        self.autoCommit = autoCommit
        self.__transactionON = False
        self.__tranactionCursor = None
        self.__dictCursor=dictCursor
        try:
            self.conn = psycopg2.connect(
                host=server, database=database, user=username, password=password, port=port)
            if (self.conn):
                self.conn.set_session(autocommit=autoCommit)
        except Exception as inst:
            print(inst.args)
            return None

    def OPstatus(self):
        if (self.conn):
            msg = ""
            if (self.conn.status == psycopg2.extensions.STATUS_READY):
                msg = "STATUS_READY"
            elif (self.conn.status == None):
                msg = "CLOSED"
            elif (self.conn.status == psycopg2.extensions.STATUS_BEGIN):
                msg = "STATUS_BEGIN"
            elif (self.conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION):
                msg = "STATUS_IN_TRANSACTION"
            elif (self.conn.status == psycopg2.extensions.STATUS_PREPARED):
                msg = "STATUS_PREPARED"
            return {"code": self.conn.status, "msg": msg}

    def closed(self):
        return bool(self.conn.closed)

    def commit(self):
        if (not self.autoCommit):
            return self.conn.commit()

    def close(self):
        if self.conn.status is not None:
            return self.conn.close()

    def info(self):
        if (self.conn):
            return self.conn.info.dbname
        else:
            return {"error3": "not connected"}

    def execFunc(self, function, params):
        if (self.conn):
            try:
                if self.__dictCursor:
                    cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                else:
                    cursor = self.conn.cursor()
                cursor.callproc(function, params)
                result = cursor.fetchone()
                if (len(result) == 1):
                    result = result[0]
                return result
            except Exception as inst:
                # cursor.close()
                return inst
            finally:
                cursor.close()

    def execProc(self, procedure, params):
        if (self.conn):
            try:
                if self.__dictCursor:
                    cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                else:
                    cursor = self.conn.cursor()
                paramsList = ""
                for param in params:
                    paramsList += "%s,"
                if (paramsList[-1] == ","):
                    paramsList = paramsList[:-1]
                cursor.execute("call {}({});".format(
                    procedure, paramsList), tuple(params))
                resp = cursor.fetchone()
                if (resp):
                    raise Exception(str(resp))
                cursor.close()
                return resp
            except Exception as inst:
                cursor.close()
                return inst.args

    def startTransaction(self):
        if (self.autoCommit):
            raise Exception("not in transaction mode")
        if (self.__transactionON):
            raise Exception("transaction already started")
        if not self.conn:
            return "error"
        self.__transactionON = True
        self.__tranactionCursor = self.conn.cursor()

    def addTransaction(self, proc, params=[]):
        if (self.autoCommit):
            raise Exception("not in transaction mode")
        if (not self.__transactionON):
            raise Exception("transaction not started")
        paramsList = ""
        for param in params:
                paramsList += "%s,"
        if (paramsList[-1] == ","):
                paramsList = paramsList[:-1]
        try:
            self.__tranactionCursor.execute("call {}({});".format(proc, paramsList), tuple(params))
            self.__tranactionCursor.fetchone()
        except Exception as exp:
            if str(exp)!="no results to fetch":
                raise Exception("error2", exp)
            else:
                return True

    def commitTransaction(self):
        if self.__transactionON:
            self.conn.commit()
            self.__tranactionCursor.close()

    def execTrans(self, transactions=[]):
        if (self.autoCommit):
            raise Exception("not in transaction mode")
        if (not type(transactions) == list):
            raise Exception(
                "transactions is not iterable [{'proc':'procName','params':[paramsList]},...]")
        if self.__dictCursor:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            cursor = self.conn.cursor()
        for tr in transactions:
            paramsList = ""
            proc = tr["proc"]
            if (not proc):
                raise Exception(
                    "need proc name {'proc':'procName','params':[paramsList]}")
            for param in tr["params"]:
                paramsList += "%s,"
            if (paramsList[-1] == ","):
                paramsList = paramsList[:-1]
            cursor.execute("call {}({});".format(
                proc, paramsList), tuple(tr["params"]))
            resp = cursor.fetchone()
            if str(resp)!="no results to fetch":
                raise Exception("error1", str(resp))
        self.conn.commit()
        self.conn.close()

    def exec(self, sqlQuery,resultExist=True):
        if (self.conn):
            try:
                if self.__dictCursor:
                    cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                else:
                    cursor = self.conn.cursor()
                cursor.execute(sqlQuery)
                if resultExist:
                        return cursor.fetchone()
            except Exception as exp:
                return exp.args
