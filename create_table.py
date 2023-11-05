from google.cloud import bigquery

def create_bigquery_dataset(credentials_path, dataset_id, location="asia-east1"):
    client = bigquery.Client.from_service_account_json(credentials_path)
    dataset_ref = client.dataset(dataset_id)

    try:
        dataset = client.get_dataset(dataset_ref)
    except Exception as e:
        dataset = None

    if not dataset:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)

def create_bigquery_table(credentials_path, dataset_id, table_id, schema, timestamp_field_name="timestamp",time_partitioning=False):
    client = bigquery.Client.from_service_account_json(credentials_path)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
    except Exception as e:
        table = None

    if not table:
        table = bigquery.Table(table_ref, schema=schema)
        if time_partitioning:
            table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=timestamp_field_name
            )
        client.create_table(table)

if __name__ == "__main__":
    credentials_path = "bq.json"
    dataset_id = "mlops"

    # Define schema 
    schema = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("score", "FLOAT")
    ]

    # Create machine_log table
    create_bigquery_dataset(credentials_path, dataset_id)
    create_bigquery_table(credentials_path, dataset_id, "abnormal",schema)

 