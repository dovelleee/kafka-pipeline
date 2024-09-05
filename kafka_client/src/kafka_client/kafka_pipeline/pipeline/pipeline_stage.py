from abc import ABC, abstractmethod
from kafka_client.kafka_client import KafkaProducerAvro, KafkaConsumerAvro
from kafka_client.cluster_admin import ClusterAdmin
from kafka_client.kafka_config import KafkaConfig, KafkaTopicConfig
from kafka_client.schema_registry import SchemaRegisteryTopicClient
from typing import Union, Callable
import time

class PipelineStageConsumer(KafkaConsumerAvro):
    '''
        :param str bootstrap_server: A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. This list should be in the form `host1:port1,host2:port2,...`
    '''
    def __init__(self, topic: str, group_id: int, bootstrap_server: str, sr_url: str, callback_fn: Callable):
        super().__init__(topic, group_id, bootstrap_server, sr_url)
        self._cb = callback_fn

    def handle_message(self, msg):
        return self._cb(msg)

class PipelineStage(ABC):
    def __init__(self, kafka_config: KafkaConfig,
                 input_topic: Union[str, None],
                 output_topic: Union[str, None],
                 consumer_group_id: Union[str, None],
                 create_if_not_exist: bool = False,
                 output_topic_config: KafkaTopicConfig = None):
        
        if input_topic == None and output_topic == None:
            raise Exception('both input_topic and output_topic cannot be None')
        if input_topic != None and consumer_group_id == None:
            raise Exception('consumer_group_id cannot be None while input_topic is provided')
        
        self._config = kafka_config
        if create_if_not_exist and output_topic:
            if not output_topic_config:
                raise Exception('output_topic_config is not provided')
            self._create_kafka_topic(output_topic_config)

        self._input, self._output = None, None
        if input_topic:
            self._input = PipelineStageConsumer(input_topic, consumer_group_id, self._config.kafka_server,
                                                self._config.sr_url, self._on_input)
        if output_topic:
            self._output = KafkaProducerAvro(output_topic, self._config.kafka_server, self._config.sr_url)

    def start(self):
        if self._input:
            self._wait_for_input_topic()
            self._input.listen()

    def _wait_for_input_topic(self, timeout: int=30):
        admin = ClusterAdmin(self._config.kafka_rest_url)
        clusters = admin.get_clusters()
        admin.select_cluster(clusters[0])
        topic_exists = False
        sleep_dt = 1
        for t in range(0, timeout, sleep_dt):
            print(self._input._topic[0], t, flush=True)
            if admin.check_topic(self._input._topic[0]):
                topic_exists = True
                print('input topic exists', flush=True)
                break
            time.sleep(sleep_dt)
        if not topic_exists:
            raise Exception('Timeout when connecting to the input topic')

    def _create_kafka_topic(self, topic_config: KafkaTopicConfig):
        # We first create the topic and then regiter its corresponding 
        # schema (the structure of the value of the messages in that topic) into the schema registry
        admin = ClusterAdmin(self._config.kafka_rest_url)
        clusters = admin.get_clusters()
        if len(clusters) < 1:
            raise Exception('There must be at least 1 cluster in this Kakfa Deployment')
        admin.select_cluster(clusters[0])
        admin.create_topic(topic_name=topic_config.topic_name,
                           n_partitions=topic_config.n_partitions,
                           n_replications=topic_config.n_replications,
                           ignore_if_exists=True)
        sr = SchemaRegisteryTopicClient(self._config.sr_url, topic_config.topic_name)
        schema = sr.load_json_shcema(topic_config.sr_json_path)
        sr.register_schema(schema)


    def _on_input(self, msg):
        print('on input called', flush=True)
        x = self.transform(msg)
        self._to_output(msg.key(), x)

    @abstractmethod
    def transform(self, msg):
        pass

    def _to_output(self, key, value):
        if self._output:
            self._output.publish(key, value)

class SourcePipelineStage(PipelineStage, ABC):
    def __init__(self, kafka_config: KafkaConfig, output_topic: Union[str, None],
                 create_if_not_exist: bool = False, output_topic_config: KafkaTopicConfig = None):
        super().__init__(kafka_config, None, output_topic, None, create_if_not_exist, output_topic_config)
    
    def transform(self, msg):
        return None
    
    @abstractmethod
    def start(self):
        pass

    def to_output(self, key, value):
        return self._to_output(key, value)
