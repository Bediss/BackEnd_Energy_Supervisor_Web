from celery import Celery
from typing import Union
import time
class Back_worker_bridge(object):
    def __init__(self, rabbitUsername: str, rabbitPassword: str, rabbitAddress: str, oneTimeUse=True, backend="rpc://",result_expires:int=5000,retrys=1):
        broker = 'amqp://{}:{}@{}//'.format(rabbitUsername,
                                            rabbitPassword,
                                            rabbitAddress)
        self.__app = None
        self.__backend = backend
        self.__broker = broker
        self.__oneTimeUse = oneTimeUse
        self.__retrys=retrys+1
        self.__result_expires=result_expires
        if self.__oneTimeUse is False:
            
            self.__app = Celery(backend=backend, broker=broker,result_expires=result_expires)
            # self.__app.conf.update(result_expires=result_expires)
  
    def iot_inner(self, task):
        """
            send task to iot_inner
            \n
            {
                    ml:list[
                            dict{
                                    m_code:str,
                                    m_name:str
                                }
                            ]
                    cl:list[
                            dict{
                                Code_Compteur:str,
                                Le_Compteur:str
                                }
                            ]
                    tl:list[
                            dict{
                                SQL:str,
                                SQLc:str
                                }
                            ]
                    cross_tab:str
                    retour:str
            }
        """
        if self.__oneTimeUse is True:
            self.__oneTimeInit(self.__result_expires)
        work = self.__app.send_task("iot_inner", args=[task],queue="iot_queue")
        while not work.ready():
            time.sleep(0.5)
            pass
        result = work.get()
        if self.__oneTimeUse:
            self.__app.close()
        return result
 
    def insert_iot(self,task):
        if self.__oneTimeUse:
            self.__oneTimeInit(self.__result_expires)
        work = self.__app.send_task("insert_iot", args=[task],countdown=self.__retrys,queue="iot_queue")
        while not work.ready():
            time.sleep(0.1)
            pass
        result = work.get()
        if self.__oneTimeUse:
            self.__app.close()
        return result
    
    def close(self):
        if self.__app:
            self.__app.close()

    def __oneTimeInit(self,result_expires):
        self.__app = Celery(backend=self.__backend,
                            broker=self.__broker, set_as_current=False,result_expires=result_expires)
        # self.__app.conf.update(result_expires=result_expires)



        # self.__app.conf.result_expires = 60
        # self.__app.conf.event_queue_expires=60
