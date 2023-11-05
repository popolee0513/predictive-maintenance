from confluent_kafka import Consumer
import json
import pandas as pd
import joblib
from google.cloud import storage
from google.cloud import bigquery
from config import config

def setup_bigquery_client(credentials_path):
    return bigquery.Client.from_service_account_json(credentials_path)

def download_pkl_file_from_gcs(bucket_name, source_blob_name, service_account_key_file):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename('model.pkl')
    model_object =  joblib.load('model.pkl')

    return model_object

def consume_kafka_messages(kafka_topic):
    kafka_config = {
    'bootstrap.servers': config['bootstrap.servers'],  # Kafka broker address
    'sasl.username': config['sasl.username'],
    'sasl.password': config['sasl.password'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'group.id': 'group_000'    
    }

    consumer = Consumer(kafka_config)
    consumer.subscribe([kafka_topic])
    
    return consumer

def insert_to_bigquery(client, dataset_id, table_id, message):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    errors = client.insert_rows_json(table, message)

    if errors:
        print("Errors occurred while streaming data:")
        for error in errors:
            print(error)

def main(credentials_path,  kafka_topic, dataset_id, table_id, bucket_name, source_blob_name):
    client = setup_bigquery_client(credentials_path)
    consumer = consume_kafka_messages(kafka_topic)
    model = download_pkl_file_from_gcs(bucket_name, source_blob_name,credentials_path)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            message_value = json.loads(msg.value().decode('utf-8'))
            timestamp = message_value['timestamp']
 
            del message_value['timestamp']
            del message_value['window']
            
            ### make the prediction
            message_value = pd.DataFrame(message_value, index=[0])
            prob = model.predict_proba(message_value)[0][1]
            
            if prob > 0.5:
                data = {'timestamp':timestamp , 'score': prob }
                insert_to_bigquery(client, dataset_id, table_id, json.dumps(data) )

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    credentials_path = "bq.json"
    kafka_topic = 'final_output'
    dataset_id = 'mlops'
    table_id = 'abnormal'
    bucket_name = 'mlops-404108-model-bucket'
    source_blob_name = 'latest_model.pkl'
    
    main(credentials_path,  kafka_topic, dataset_id, table_id, bucket_name, source_blob_name)