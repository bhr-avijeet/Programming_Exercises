from kafka import KafkaConsumer
if __name__ == '__main__':
    consumer = KafkaConsumer("simple-producer-consumer",
                             bootstrap_servers=["localhost:9092"])
    for msg in consumer:
        print(msg)