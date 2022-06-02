from kafka import KafkaProducer

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
    key_bytes = bytes("key1", encoding='utf-8')
    value_bytes = bytes("Message2", encoding='utf-8')
    producer.send(topic="simple-producer-consumer", key=key_bytes, value=value_bytes)
    producer.close()
    producer.flush()
