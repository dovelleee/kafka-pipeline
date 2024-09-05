from kafka_client.kafka_client import KafkaConsumerAvro


config = {
    'kafka-host': 'dev-server.local',
    'sr-url': 'http://dev-server.local:8081',
    'kafka-rest-url': 'http://dev-server.local:8082',
    'topic-name': 'test_topic'
}

class TestKafkaConsumer(KafkaConsumerAvro):
    def handle_message(self, msg):
        print('%% %s [%d] at offset %d with key %s:' %
                (msg.topic(), msg.partition(), msg.offset(),
                str(msg.key())), flush=True)
        print(msg.value(), flush=True)

consumer = TestKafkaConsumer(topic=config['topic-name'], group_id=1,
                             bootstrap_server_host=config['kafka-host'],
                             sr_url=config['sr-url'])
consumer.listen()