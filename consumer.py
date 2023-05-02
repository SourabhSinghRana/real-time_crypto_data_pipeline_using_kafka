#import os
import pandas as pd
import boto3
import json
import datetime


from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient



API_KEY = '<confluent-kafka-api-key>'
ENDPOINT_SCHEMA_URL  = '<endpoint url>'
API_SECRET_KEY = '<confluent-kafka-api-secret-key>'
BOOTSTRAP_SERVER = '<url>'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '<schema-reg-key>'
SCHEMA_REGISTRY_API_SECRET = '<schema-reg-secret-key>'




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



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"}


class CryptoRec:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record=record
        
    @staticmethod
    def dict_to_CryptoRec(data:dict,ctx):
        return CryptoRec(record=data)

    def __str__(self):
        return f"{self.record}"
    


s3 = boto3.client('s3')
bucket_name = "your-bucket-name"
def consumeData(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    json_deserializer = JSONDeserializer(schema_str,
                                        from_dict=CryptoRec.dict_to_CryptoRec)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                    'group.id': 'group1',
                    'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            
            #to hold tranformed data
            transform_data = {}
            
            cryptoRecord = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if cryptoRecord is not None:
                #print("User record {}: crypto_Data: {}\n".format(msg.key(), cryptoRecord))
                transform_data['SYSTEM_INSERTED_TIMESTAMP'] = datetime.datetime.fromtimestamp(cryptoRecord.record['SYSTEM_INSERTED_TIMESTAMP'] / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
                transform_data['RANK'] = int(cryptoRecord.record['RANK'])
                transform_data['NAME'] = cryptoRecord.record['NAME']
                transform_data['SYMBOL'] = cryptoRecord.record['SYMBOL']
                transform_data['PRICE'] = float((cryptoRecord.record['PRICE'].replace('$', '').replace(',', '').replace(' ', '')))
                transform_data['PERCENT_CHANGE_24H'] = float(cryptoRecord.record['PERCENT_CHANGE_24H'].replace('%', '').replace(',', '').replace(' ', ''))
                transform_data['VOLUME_24H'] = float(cryptoRecord.record['VOLUME_24H'].replace('$', '').replace('B', 'E9').replace('M', 'E6').replace(',', '').replace(' ', ''))
                transform_data['MARKET_CAP'] = float(cryptoRecord.record['MARKET_CAP'].replace('$', '').replace('B', 'E9').replace('M', 'E6').replace(',', '').replace(' ', ''))
                transform_data['CURRENCY'] = 'USD'
                
                json_str = json.dumps(transform_data)
                #print(json_str)
                file_name = "real_time_data/top_100_crypto_data_" + str(cryptoRecord.record['SYSTEM_INSERTED_TIMESTAMP']) + '_' + str(cryptoRecord.record['RANK']) + '.json'

                response = s3.put_object(
                    Bucket=bucket_name,
                    Key=file_name,
                    Body=json_str)    
                print("file_uploaded: ",file_name)
        except KeyboardInterrupt:
            break

    consumer.close()    




def main():
        
    topic = 'top_100_crypto'
    print("starting consumer: ",topic)
    consumeData(topic)
    

main()	
	
	
