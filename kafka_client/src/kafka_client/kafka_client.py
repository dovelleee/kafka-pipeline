from confluent_kafka import Producer, Consumer, SerializingProducer, KafkaException
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from .schema_registry import SchemaRegisteryTopicClient
import logging
import sys

'''
This doc was helpful: https://medium.com/@mrugankray/create-avro-producer-for-kafka-using-python-f9029d9b2802
'''

class KafkaProducer:
    def __init__(self, topic: str, bootstrap_server_host: str):
        self._bs_host = bootstrap_server_host
        self._topic = topic
        producer_conf = { 'bootstrap.servers': self._bs_host }
        self._p = Producer(**producer_conf)

    def on_delivered(self, err, msg):
        pass

    def publish(self, key: str, message):
        self._p.poll(0)
        self._p.produce(self._topic, key=key, value=message, callback=self.on_delivered)
        self._p.flush()


class KafkaConsumer:
    def __init__(self, topic: str, group_id: int, bootstrap_server_host: str):
        self._topic = topic
        self._bs_host = bootstrap_server_host
        consumer_conf = {'bootstrap.servers': self._bs_host,
                         'group.id': group_id,
                         'session.timeout.ms': 6000,
                         'auto.offset.reset': 'earliest',
                         'enable.auto.offset.store': False}
        self._init_logger()
        self._c = Consumer(consumer_conf, logger=self._logger)
        self._c.subscribe(self._topic, on_assign=self._on_assign)

        
    def _init_logger(self):
        logger = logging.getLogger('consumer')
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
        logger.addHandler(handler)
        self._logger = logger

    def _on_assign(self, a, b):
        pass

    def _handle_message(self, msg):
        # Proper message
        sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                        (msg.topic(), msg.partition(), msg.offset(),
                        str(msg.key())))
        print(msg.value())
        # Store the offset associated with msg to a local cache.
        # Stored offsets are committed to Kafka by a background thread every 'auto.commit.interval.ms'.
        # Explicitly storing offsets after processing gives at-least once semantics.
        self._c.store_offsets(msg)
        self.handle_message(msg)

    def handle_message(self, msg):
        pass

    def listen(self):
        try:
            while True:
                msg = self._c.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    self._handle_message(msg)

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

        finally:
            # Close down consumer to commit final offsets.
            self._c.close()


class KafkaProducerAvro(KafkaProducer):
    '''
        - topic: name of kafka topic
    '''


    def __init__(self, topic: str, bootstrap_server_host: str, sr_url: str):
        self._topic = topic
        self._sr_client = SchemaRegisteryTopicClient(sr_url=sr_url, topic_name=topic)
        self._msg_schema = self._sr_client.get_schema_from_schema_registry()
        self._avro_serializer = AvroSerializer(schema_registry_client=self._sr_client._sr_client,
                                               schema_str=self._msg_schema.schema.schema_str,
                                               conf={'auto.register.schemas': False})
        self._bs_host = bootstrap_server_host
        self._sr_url = sr_url
        producer_conf = {
            'bootstrap.servers': self._bs_host,
            'security.protocol': 'plaintext',
            'value.serializer': self._avro_serializer,
            'delivery.timeout.ms': 120000,
            'enable.idempotence': 'true'
        }
        self._p = SerializingProducer(producer_conf)

    def on_delivered(self, err, msg):
        print('hello')
        return super().on_delivered(err, msg)

    def publish(self, key: str, message):
        self._p.poll(0)
        self._p.produce(self._topic, key=key, value=message, on_delivery=self.on_delivered)
        self._p.flush()

class KafkaConsumerAvro(KafkaConsumer):
    def __init__(self, topic: str, group_id: int, bootstrap_server_host: str, sr_url: str):
        super().__init__(topic, group_id, bootstrap_server_host)
        self._sr_client = SchemaRegisteryTopicClient(sr_url=sr_url, topic_name=topic)
        self._msg_schema = self._sr_client.get_schema_from_schema_registry()
        self._avro_deserializer = AvroDeserializer(schema_registry_client=self._sr_client._sr_client,
                                               schema_str=self._msg_schema.schema.schema_str)
        self._sr_url = sr_url

    def _handle_message(self, msg):
        decoded_key = bytes.decode(msg.key())
        decoded_msg = self._avro_deserializer(msg.value(), None)
        msg.set_key(decoded_key)
        msg.set_value(decoded_msg)
        return super()._handle_message(msg)
