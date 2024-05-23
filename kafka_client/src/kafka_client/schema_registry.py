from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema

class SchemaRegisteryTopicClient:
    '''
        This class handles the complexities of creating AVRO schema for a Kafka topic and gives a high-level interface to 
        create, update, and retreive the schema for a given Kafka topic. Here, we assume that every topic has only schema.
    '''
    def __init__(self, sr_url: str, topic_name: str):
        self._schema_registry_url = sr_url
        self._kafka_topic = topic_name
        self._schema_registry_subject = f"{topic_name}-value"
        self._sr_client = SchemaRegistryClient({'url': sr_url})

    @staticmethod
    def load_json_shcema(path: str):
        with open(path, 'r') as f:
            return f.read()

    def get_schema_from_schema_registry(self):
        latest_version = self._sr_client.get_latest_version(self._schema_registry_subject)
        return latest_version

    def register_schema(self, schema_str):
        schema = Schema(schema_str, schema_type="AVRO")
        schema_id = self._sr_client.register_schema(subject_name=self._schema_registry_subject, schema=schema)
        return schema_id

    def update_schema(self, schema_str):
        versions_deleted_list = self._sr_client.delete_subject(self._schema_registry_subject)
        schema_id = self.register_schema(schema_str)
        return schema_id
