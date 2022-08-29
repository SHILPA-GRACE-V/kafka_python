from multiprocessing.sharedctypes import Value
from random import randint
from threading import Timer
from kafka import KafkaProducer 
import datetime
from time import sleep
import random
topic_name="TempMonitoringSystem"
server="localhost:9092"
while True:
    period = datetime.datetime.now()
    if((period.second % 5) == 0):
        temp = random.randrange(10, 40, 3)
        producer = KafkaProducer(bootstrap_servers = [server])
        producer=KafkaProducer()
        message=producer.send(topic_name, bytes(str(temp),encoding='utf-8'))
        metadata = message.get()
        print(temp)
        sleep(1)
