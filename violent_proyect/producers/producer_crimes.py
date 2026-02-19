print("Producers crimes")

from kafka import KafkaProducer
import json 
from collections import defaultdict
from datetime import datetime, timezone
import time 
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

limpio_long = "/mnt/c/Users/zabu/Desktop/violent_proyect/violent_long_clean.csv"

df = pd.read_csv(limpio_long)

for _, row in df.iterrows():
    event = {
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "crime_type": row.get("crime_type"),
        "JURISDICTION": row.get("JURISDICTION"),
        "severity": row.get("severity"),
        "rate_per_100k": row.get("rate_per_100k")
    }

    producer.send(
        topic="crime_event",
        value=event
    )
    print(f"Enviando el evento: {event['crime_type']}")

    time.sleep(1)