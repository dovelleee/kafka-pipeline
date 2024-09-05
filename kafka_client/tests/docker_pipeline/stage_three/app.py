from kafka_client.kafka_config import KafkaConfig, KafkaTopicConfig
from multiprocessing import Process
from time import sleep
from kafka_client.kafka_pipeline.pipeline.kafka_pipeline import MultiProcessPipeline
from kafka_client.kafka_pipeline.pipeline.pipeline_stage import PipelineStage, SourcePipelineStage
from time import sleep
from uuid import uuid4
import os

SCRIPT_DIR = os.path.dirname(__file__)

kafka_config = KafkaConfig(
    kafka_server='dev-server.local:29094',
    kafka_rest_url='http://dev-server.local:8082',
    sr_url='http://dev-server.local:8081')

class StageThree(PipelineStage):
    def __init__(self, kafka_config: KafkaConfig):
        
        input_topic = 'test_stage_two_topic'
        output_topic = 'test_stage_three_topic'
        output_topic_config = KafkaTopicConfig(
            topic_name=output_topic,
            n_partitions=2, n_replications=1,
            sr_json_path=os.path.join(SCRIPT_DIR, './schema/stage_three.json'))
        consumer_group_id = 'stage_three_g1'
        super().__init__(kafka_config, input_topic, output_topic, consumer_group_id, True, output_topic_config)

    def transform(self, msg):
        print(f'stage three:\t\t{msg.key()}:{msg.value()}', flush=True)
        return {'postid': msg.value()['postid'], 'ppm': 200}


if __name__ == "__main__":
    StageThree(kafka_config).start()