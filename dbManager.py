import psycopg2
from psycopg2 import extras
import select
import threading
import signal
import asyncio
from retrying import retry
import time

class threadJob(threading.Thread):
    def __init__(self, func, args=None,daemon=True):
        self._stop_event = threading.Event()
        self.func = func
        self.args = args

        threading.Thread.__init__(self)
        self.daemon=daemon

    def run(self):
        self.func(self.args)

    def stop(self):
        self._stop_event.set()

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
    def __init__(self, database, username="postgres", password="root", server="localhost", port=5432, autoCommit=True,dictCursor=False,autoConnect=True,application_name=''):
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
        self.loop=dict()
        self.params = {
            "server": server,
            "database": database,
            "username": username,
            "password": password,
            "port": port,
            "autoCommit":autoCommit,
            "application_name":application_name
        }
        try:
            if autoConnect is True:
                self.conn=self.__connect()

                if (self.conn):
                    self.conn.set_session(autocommit=autoCommit)
        except Exception as inst:
            print(inst.args)
            return None
    
    def __connect(self):
        try:
            server = self.params.get("server")
            database = self.params.get("database")
            username = self.params.get("username")
            password = self.params.get("password")
            port = self.params.get("port")
            autoCommit=self.params.get("autoCommit")
            application_name=self.params.get("application_name")
            conn = psycopg2.connect(host=server, database=database, user=username, password=password, port=port,application_name=application_name)
            if (conn):
                conn.set_session(autocommit=autoCommit)
            return conn
        except Exception as exp:
            print(exp)
            return None

    def OPstatus(self):
        if self.conn is None:
                return {"code": None, "msg": 'CLOSED'}
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
        if self.conn is None:
            return True
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
            except psycopg2.errors.UndefinedFunction as exp:
                return dict({
                    "error":True,
                    "args":exp
                })
            except Exception as exp:
                # cursor.close()
                return dict({"error":True,"args":exp})

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
                cursor.execute("call {}({});".format(procedure, paramsList), tuple(params))
                resp = cursor.fetchone()
            except psycopg2.ProgrammingError as exp:
                return {"error":False}
            except Exception as inst:
                # if inst.args[0] == "no results to fetch":
                #     return {"error":False}
                # else:
                    return {"error":True,"args":inst.args}
            finally:
                if cursor and cursor.closed is False:
                    cursor.close()

    def startTransaction(self):
        if (self.autoCommit):
            raise Exception("not in transaction mode")
        if (self.__transactionON):
            raise Exception("transaction already started (1)")
        if self.__tranactionCursor:
            raise Exception("transaction already started (2)")
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
            if exp.args[0] == "no results to fetch":
                return {"error":False}
            else:
                return {"error":True,"args":exp.args}   

    def commitTransaction(self):
        if self.__transactionON:
            result=self.conn.commit()
            self.__tranactionCursor.close()
            return result

    def rollbackTransaction(self):
        if self.__transactionON:
            result=self.conn.rollback()
            self.__tranactionCursor.close()
            return result

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

    def exec(self, sqlQuery,params=None,resultExist=True,many=False,count=False):
        if (self.conn):
            try:
                if self.__dictCursor:
                    cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                else:
                    cursor = self.conn.cursor()
                cursor.execute(query=sqlQuery,vars=params)
                if resultExist:
                    if many is False:
                        return cursor.fetchone()
                    elif many is True and count is False :
                        return cursor.fetchall()
                    else:
                        return cursor.fetchmany(count)
            except Exception as exp:
                return exp.args

    def export_csv(self,sqlQuery,destination):
        if (self.conn):
            try:
                cursor = self.conn.cursor()
                outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sqlQuery)
                cursor.execute(sqlQuery)
                with open(destination, 'w') as f:
                    cursor.copy_expert(outputquery, f)
            except Exception as exp:
                return exp.args
            finally:
                cursor.close()
    
    @retry(wait_fixed=1000)
    def listen(self, channel, callback, params):
        """
        channel str :channel to listen to
        callback function : exec on every notify with selected channel
                                callback(params:any,payload:any)
        params: any : params to be passed to callback
        """
        
        conn = self.__connect()

        if conn is None:
            raise Exception("go reconnect")
            
        else:
            
            cursor = conn.cursor()
            if cursor is not None:
                
                cursor.execute("LISTEN {};".format(channel))
                print("Waiting for notifications on channel '{}'".format(channel))
                while True:
                    if not select.select([conn], [], [], 5) == ([], [], []):
                        conn.poll()
                        while conn.notifies:
                            notify = conn.notifies.pop(0)

                            if channel == notify.channel:
                                callback(params, payload=notify.payload)
            else:
                raise Exception("retry")

    def listenV2(self,channel,callback,params,separateThread:False,autoConn=False):
        """
        channel str :channel to listen to
        callback function : exec on every notify with selected channel
                                callback(params:any,payload:any)
        params: any : params to be passed to callback
        """

        def curAndListen():
                self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = self.conn.cursor()
                cursor.execute("LISTEN {};".format(channel))
                print('\033[95m'+"Waiting for notifications on channel '{}'".format(channel)+"\033[0m")
        try:
            if separateThread is True:
                
                @retry(wait_fixed=1000)
                def listening(args=None):
                    if autoConn is True:
                        self.conn=self.__connect()

                    curAndListen()
                    while 1:
                        try:
                            if not select.select([self.conn],[],[],5) == ([],[],[]):
                                self.conn.poll()
                                for notify in self.conn.notifies:
                                    if channel==notify.channel:
                                        callback(params=params,payload=notify.payload)
                                    self.conn.notifies.clear()
                        except:
                            print("listener connection terminated.")
                            self.close()
                            _retry=True
                            while _retry:
                                time.sleep(1)
                                self.conn=self.__connect()
                                if not self.closed():
                                    _retry=False
                            curAndListen()
                            continue
                self._th=threadJob(func=listening)
                self._th.start()

                self.loop={"active":True,"th":True}
            else:
                self.loop={"active":True,"th":False}
                if autoConn is True:
                    self.conn=self.__connect()

                curAndListen()

                while 1:
                    try:
                        if not select.select([self.conn],[],[],5) == ([],[],[]):
                            self.conn.poll()
                            while self.conn.notifies:
                                notify = self.conn.notifies.pop(0)
                                if channel==notify.channel:
                                    callback(params,payload=notify.payload)
                    except:
                        self.close()
                        self.conn=self.__connect()
                        curAndListen()
                        continue
        except Exception as exp:
            self.close()
            raise exp
    def stopListen(self):
        #only for thread now
        if self.loop.get("active",False) is True:
            if self.loop.get("th",False) is True:
                self._th.stop()
            else:
                ""
            
    