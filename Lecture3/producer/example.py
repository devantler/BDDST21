from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='kafka:9092')
for _ in range(5):
    producer.send('foo', key=b'foo', value=b'bar')
producer.flush()