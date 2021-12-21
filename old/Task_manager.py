import json
import uuid
import os
import time
import sys
import pika


class TaskManager(object):
    def __init__(self, taskQueue, exchangeName="", server="localhost",port=5672,username="guest",password="guest"):
        #taskQueue is required
        if not taskQueue:
            raise Exception("taskQueue is required") 
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(server, port, '/', credentials)
        self.connection = pika.BlockingConnection(parameters)
        
        self.exchangeName = exchangeName.strip()
        self.taskQueue = taskQueue
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.taskQueue, durable=True)
        # if costum exchange, declare it if dont exists and bind it to queue
        if self.exchangeName:
            self.channel.exchange_declare(exchange=self.exchangeName,
                             durable=True, exchange_type='direct')
            self.channel.queue_bind(exchange=self.exchangeName, queue=self.taskQueue)
            # bind exchange and response queue (powershell is dump)
          

    def on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = body
            ch.stop_consuming()
            # ch.queue_unbind(self.callback_queue)

    def execTask(self, task,expire=20000):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.expire=expire


        result = self.channel.queue_declare(queue="", auto_delete=True)
        self.callback_queue = result.method.queue

        self.channel.queue_bind(exchange=self.exchangeName, queue=self.callback_queue)

        self.consumerTag=self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=False)

        self.channel.basic_publish(
            exchange=self.exchangeName,
            routing_key=self.taskQueue,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
                expiration=str(self.expire)
            ),

            body=json.dumps(task))

        expireCount=0
        while self.response is None:
            self.connection.process_data_events()
            time.sleep(0.01)
            #calc request timeout
            expireCount+=1
            if expireCount==self.expire:
                self.response ='{"errorrrrrrrrrrrrrrrrrrrr":"timeout"}'

        return self.response