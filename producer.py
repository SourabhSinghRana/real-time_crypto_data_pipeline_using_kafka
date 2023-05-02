from bs4 import BeautifulSoup
import requests
import time
import datetime
import pandas as pd
import argparse
import json
from json import dumps
from time import sleep

from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer



API_KEY = '<confluent-kafka-api-key>'
ENDPOINT_SCHEMA_URL  = '<endpoint url>'
API_SECRET_KEY = '<confluent-kafka-api-secret-key>'
BOOTSTRAP_SERVER = '<url>'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '<schema-reg-key>'
SCHEMA_REGISTRY_API_SECRET = '<schema-reg-secret-key>'


parser = argparse.ArgumentParser(description='To gett arguments passed during run time')
parser.add_argument('topic', help='topic help')
parser.add_argument('time', help='time help')
args = parser.parse_args()

topic = args.topic
time = int(args.time)



#config function
def sasl_conf():
    
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

#config function
def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"}

#Error detector function. Gives report if data is sent succesfully or not
def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))



#function for scraping data when called
def scrape_data(url):
    
	allRecordsCombined = []

	for page in range(1,3):
		# Make a request to the website
		response = requests.get(url+str(page))
		current_timestamp = datetime.datetime.now()
	
		soup = BeautifulSoup(response.content, 'html.parser')

		# Find the table containing the top 100 cryptocurrencies
		treeTag = soup.find_all('tr')

		#print(treeTag)

		for tree in treeTag[1:]:
			rank = tree.find('td',{'class': 'css-w6jew4'}).get_text()
			name = tree.find('p',{'class': 'chakra-text css-rkws3'}).get_text()
			symbol = tree.find('span',{'class': 'css-1jj7b1a'}).get_text()
			market_cap = tree.find('td',{'class':'css-1nh9lk8'}).get_text()
			change_24h = ""
			price_arr = str(tree.find('div',{'class':'css-16q9pr7'}).get_text())
			if('-' in price_arr):
				price_arr = price_arr.split('-')
				change_24h = '-'+price_arr[1]
			else:
				price_arr = price_arr.split('+')
				change_24h = '+'+price_arr[1]
			price = price_arr[0]
			volume_24 = tree.find('td',{'class':'css-1nh9lk8'}).get_text()
			

			#print("Rank: ", rank)
			#print("NAME: ", name)
			#print("symbol: ", symbol)
			#print("price: ", price)
			#print("market_cap: ", market_cap)
			#print("volume_24: ", volume_24)
			#print("change_24h: ", change_24h)
			
			allRecordsCombined.append([current_timestamp, rank, name, symbol, price, change_24h, volume_24, market_cap])
		
	columns = ['SYSTEM_INSERTED_TIMESTAMP', 'RANK','NAME', 'SYMBOL', 'PRICE', 'PERCENT_CHANGE_24H','VOLUME_24H', 'MARKET_CAP']
	df = pd.DataFrame(columns=columns, data=allRecordsCombined)
	current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
	# Convert data frame to JSON string
	json_export = df.to_json(orient='records')
	#df.to_csv('s3://coinmarketcap-bucket/raw_layer/{}.csv'.format(current_timestamp), index=False)
	#df.to_csv(f"{dag_path}/dags/output/{current_timestamp}.csv", index=False)
	#print(f"FILE created at: {dag_path}/output/{current_timestamp}.csv")
	return json.loads(json_export)
    

#class craeted to hold scraped data in object format
class CryptoRec:   
    def __init__(self,record:dict):
        self.record=record
        
    @staticmethod
    def dict_to_CryptoRec(data:dict,ctx):
        return CryptoRec(record=data)

    def __str__(self):
        return f"{self.record}"
    


#helper funtion to convert object to dict data type 
def cryptoRec_to_dict(crypto:CryptoRec, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return crypto.record


#This producer function will produce and send data to Confluent kafka	
def produceData(topic):
    
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str
    
    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, cryptoRec_to_dict)

    producer = Producer(sasl_conf())
    
    for i in range(0,time):
        url = 'https://crypto.com/price?page='
        json_file = scrape_data(url)
        #print(json_file)
        
        print("Producing user records to topic {}. ^C to exit.".format(topic))
        
        try:
            for temp_rec in json_file:
                rec=CryptoRec(temp_rec)
                producer.produce(
                            topic=topic,
                            key=string_serializer(str(uuid4())), 
                            value=json_serializer(rec, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            producer.poll(0.2)
        except ValueError:
            print("Invalid input, discarding record...")
            pass
        
        #5 sec delay
        sleep(5)

    print("\nFlushing records...")
    producer.flush()

    

def main():
        
    #topic = 'top_100_crypto'
    print("starting producer: ",topic)
    produceData(topic)  
	
	
main()	
	
	
