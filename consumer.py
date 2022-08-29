from kafka import KafkaConsumer
import sys
topic_name="TempMonitoringSystem"
server="localhost:9092"
my_consumer = KafkaConsumer( topic_name,bootstrap_servers = [server],auto_offset_reset = 'earliest'  )
try:
    for i in my_consumer:
        partition=i.partition
        print(i.value)
        #print(partition)
except KeyboardInterrupt:
    sys.exit()