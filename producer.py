import pandas as pd
import json
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import KafkaError
from config import config

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
        # Write the key of undelivered message to a text file
        with open('undelivered_keys.txt', 'a') as file:
            file.write(msg.key().decode('utf-8') + '\n')
    else:
        pass

def publish_to_kafka(conf,csv_file, kafka_topic):
    # Create a Kafka producer
    producer = Producer(conf)

    # Read the CSV file
    df = pd.read_csv(csv_file)
    df.timestamp = df.timestamp.astype('str')
    
    
    # Iterate through each row and publish to Kafka
    for index, row in df.iterrows():
        
        message_data = row.to_dict() 
        future = producer.produce(kafka_topic, key=message_data['timestamp'].encode() , value=json.dumps(message_data, default=str).encode('utf-8') , callback=delivery_report)
        producer.poll(0) # Trigger any available delivery report callbacks from previous produce() calls
    producer.flush()

if __name__ == "__main__":

    # CSV file to read
    input_csv_file = 'real_time.csv' 

    # Kafka topic to publish to
    kafka_topic = 'raw'  
    
    conf = {
    'bootstrap.servers': config['bootstrap.servers'],  # Kafka broker address
    'sasl.username': config['sasl.username'],
    'sasl.password': config['sasl.password'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN'
    
}
    
    publish_to_kafka(conf,input_csv_file, kafka_topic)