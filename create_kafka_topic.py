from datetime import datetime
import json
import requests
from time import sleep
from kafka import KafkaProducer

with open('token.json', 'r') as j:
    contents = json.loads(j.read())

token = contents['token']
base_url = 'https://cloud.iexapis.com/stable/stock/'

def assemble_url(base_url, stock, date, token):
#This function assembles a URL to call the stock price API
     return base_url+stock+'/chart/date/'+date+'?token='+token


date = datetime.today().strftime('%Y%m%d')
stock = 'AAPL'

url = assemble_url(base_url, stock, date, token)
response = requests.get(url)
data = json.loads(response.content.decode('utf-8'))

producer = KafkaProducer(bootstrap_servers='172.25.0.12:9092')
topic_name = 'AAPL'


from kafka.admin import KafkaAdminClient, NewTopic

#Create a Kafka topic for the specific stock from within our script
admin_client = KafkaAdminClient(
    bootstrap_servers="172.25.0.12:9092"
)

topic_list = []
topic_list.append(NewTopic(name=stock, num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)


#In this for loop, we publish stock prices to the topic
for d in data:
    key_bytes = bytes(d['minute'], encoding='utf-8')
    value_bytes = bytes(str(d['average']), encoding='utf-8')
    producer.send(stock, key=key_bytes, value=value_bytes)