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
    # stage one does not have any input topic, rather it generates a bunch of 
    # messages and pushes them to the output topic
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

class StageTwo(PipelineStage):
    def __init__(self, kafka_config: KafkaConfig):
        
        input_topic = 'test_stage_one_topic'
        output_topic = 'test_stage_two_topic'
        output_topic_config = KafkaTopicConfig(
            topic_name=output_topic,
            n_partitions=2, n_replications=1,
            sr_json_path=os.path.join(SCRIPT_DIR, './schema/stage_two.json'))
        consumer_group_id = 'stage_two_g1'
        super().__init__(kafka_config, input_topic, output_topic, consumer_group_id, True, output_topic_config)

    def transform(self, msg):
        print(f'stage two:\t\t{msg.key()}:{msg.value()}', flush=True)
        return {'postid': msg.value()['postid'], 'ppm': 100}


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
    mpp = MultiProcessPipeline('test_pln', [
        lambda: StageOne(kafka_config),
        lambda: StageTwo(kafka_config),
        lambda: StageThree(kafka_config)
    ], 5)
    mpp.start()
