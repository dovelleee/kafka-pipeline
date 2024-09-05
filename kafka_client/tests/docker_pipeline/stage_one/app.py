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

class StageOne(SourcePipelineStage):
    def __init__(self, kafka_config: KafkaConfig):
        output_topic = 'test_stage_one_topic'
        output_topic_config = KafkaTopicConfig(
            topic_name=output_topic,
            n_partitions=4, n_replications=1,
            sr_json_path=os.path.join(SCRIPT_DIR, './schema/stage_one.json'))
        super().__init__(kafka_config, output_topic, True, output_topic_config)
    
    def start(self):
        for i in range(100):
            msg_value = {
                'postid': f'postid_{i}',
                'title': uuid4().hex
            }
            self.to_output(f'postid_{i}', msg_value)
            sleep(0.1)


if __name__ == "__main__":
    StageOne(kafka_config).start()