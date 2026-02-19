print("consumer crimes")

from kafka import KafkaConsumer
import json

from google.cloud import bigquery
from datetime import datetime, timezone 
import uuid

consumer = KafkaConsumer(
    'crime_event',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True, 
    group_id='crimen_event',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer conectado. Esperando eventos...\n")

client = bigquery.Client(project="violent-crime-pipeline")

table_id = "violent-crime-pipeline.crime_analytics.crime_events"

for message in consumer:
    kafka_event = message.value  #evento original del producer
    
    #esto ira a bigquery

    event_timestamp = datetime.fromisoformat(
        kafka_event.get("event_timestamp").replace("Z", "+00:00")
    ).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")

    ingestion_timestamp = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f+00:00"
    )

    event = {
        "event_id": str(uuid.uuid4()),
        "crime_type": kafka_event.get("crime_type"),
        "jurisdiction": kafka_event.get("JURISDICTION"),
        "rate_per_100k": kafka_event.get("rate_per_100k"),
        "severity": kafka_event.get("severity"),
        "event_timestamp": event_timestamp,
        "ingestion_timestamp": ingestion_timestamp,
        "source": "kafka"
    }

    print("Evento recibido")
    print(f" Crime type    : {event['crime_type']}")
    print(f" jurisdiction  : {event['jurisdiction']}")
    print(f" Rate per 100k : {event['rate_per_100k']}")
    print(f" severity      : {event['severity']}")
    print(f" Timestamp     : {event['event_timestamp']}")
    print("-"*60)

    print("intentado insertar en Bigquery...")
    errors = client.insert_rows_json(table_id, [event])

    if errors:
        print("Error al insertar en BigQuery:", errors)
    else:
        print("Evento insertado en BigQuery")