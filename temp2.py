import mysql.connector
from kafka import KafkaConsumer
server_name="localhost"
db_username="root"
db_pass="Sgv3#321"
db_name="iotdb"
connection=mysql.connector.connect(
    host=server_name,
    user=db_username,
    password=db_pass,
    database=db_name
)
my_c=connection.cursor()
topic_name="TempMonitoringSystem"
my_consumer = KafkaConsumer( topic_name,bootstrap_servers = [server_name],auto_offset_reset = 'earliest'  )
try:
    for i in my_consumer:
        n=str(input("Enter a value: ")) 
        print(n)
        my_c.execute("insert into Temp(Temperature) values(%s)",(n,))
        connection.commit()
        print("Data insrted")
except KeyboardInterrupt:
    sys.exit()
