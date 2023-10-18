import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json
import random

def send_to_kafka(producer, data):
    producer.send('', value=data)

def main():
    kafka_servers = ['']  # Change the server address here
    topic = ''
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    df = pd.read_csv("data/indexProcessed.csv")
    
    while True:
        dict_stock = df.sample(1).to_dict(orient="records")[0]
        send_to_kafka(producer, dict_stock)
        sleep(1 + random.random())  # Randomize the sleep interval slightly

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()
