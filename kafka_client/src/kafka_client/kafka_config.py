from dataclasses import dataclass

@dataclass
class KafkaConfig:
    kafka_server: str
    kafka_rest_url: str
    sr_url: str

@dataclass
class KafkaTopicConfig:
    topic_name: str
    n_replications: int
    n_partitions: int
    sr_json_path: str
