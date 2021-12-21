from celery import Celery
from typing import Union

class Back_worker_bridge(object):
    def __init__(self, rabbitUsername: str, rabbitPassword: str, rabbitAddress: str, oneTimeUse=False, backend="rpc://",result_expires:int=300,retrys=1) -> None:
        
        broker = 'amqp://{}:{}@{}//'.format(rabbitUsername,
                                            rabbitPassword,
                                            rabbitAddress)
        self.__app = None
        self.__backend = backend
        self.__broker = broker
        self.__oneTimeUse = oneTimeUse
        self.__retrys=retrys+1
        self.__result_expires=result_expires
        if not self.__oneTimeUse:
            self.__app = Celery(backend=backend, broker=broker)
            self.__app.conf.update(result_expires=result_expires)
    def iot_inner(self, task)->Union[list,None,Exception]:
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
        if self.__oneTimeUse:
            self.__oneTimeInit(self.__result_expires)
        work = self.__app.send_task("iot_inner", args=[task],countdown= self.__retrys)
        while not work.ready():
            pass
        result = work.get()
        if self.__oneTimeUse:
            self.__app.close()
        return result

    def __oneTimeInit(self,result_expires):
        self.__app = Celery(backend=self.__backend,
                            broker=self.__broker, set_as_current=False)
        self.__app.conf.update(result_expires=result_expires)
