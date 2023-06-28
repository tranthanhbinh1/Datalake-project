from kafka import KafkaProducer


# Define the Kafka producer , RUN DOCKER COMPOSE UP FIRST
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Create a Kafka Producer instance
bootstrap_servers = "localhost:29092"
topic1 = "dbToS3"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def publishToKafka(topic, data):
    try:
        producer.send(topic, str.encode(data))
        producer.flush()
        print("Message published successfully to Kafka")
    except Exception as ex:
        print("Exception in publishing message to Kafka")
        print(str(ex))