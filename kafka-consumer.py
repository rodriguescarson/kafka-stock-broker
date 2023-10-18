import json
from kafka import KafkaConsumer
from s3fs import S3FileSystem
from json import loads

def consume_and_upload_to_s3(consumer, s3):
    for count, message in enumerate(consumer):
        try:
            data = message.value
            with s3.open(f"s3://<s3bucketname>_{count}.json", "w") as file:
                json.dump(data, file)
        except Exception as e:
            print(f"Error processing message {count}: {str(e)}")

def main():
    kafka_servers = [""]  # Change the Kafka server address here
    topic = ""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_servers,
        value_deserializer=lambda x: loads(x.decode("utf-8"))
    )
    s3 = S3FileSystem()

    consume_and_upload_to_s3(consumer, s3)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
