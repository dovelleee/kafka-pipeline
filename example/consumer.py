from config import config
from lib.kafka.kafka_connection import KafkaConsumerAvro
import os

SCRIPT_DIR = os.path.dirname(__file__)

c = KafkaConsumerAvro(['test_topic'], 1, 'test_topic-value')
c.listen()