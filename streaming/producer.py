from kafka import KafkaProducer


# Define the Kafka producer , RUN DOCKER COMPOSE UP FIRST
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Create a Kafka Producer instance
bootstrap_servers = "localhost:29092"
topic1 = "dbToS3"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)