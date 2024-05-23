from config import config
from lib.kafka.schema_registry import register_schema, load_schema_json
from lib.kafka.kafka_connection import KafkaProducerAvro, KafkaConsumerAvro
from lib.kafka.cluster_admin import ClusterAdmin
import glob, os, pathlib
import sched, time

SCRIPT_DIR = os.path.dirname(__file__)
KAFKA_TOPIC_NAME = 'test_topic'

## register schemas to sr
schema_registry_url = config['kafka.schema-registry.url']

for sc_file in glob.glob(os.path.join(SCRIPT_DIR, './avro-schemas', '*.json')):
    sc = load_schema_json(sc_file)
    sr_subject = pathlib.Path(sc_file).stem
    schema_id = register_schema(schema_registry_url, sr_subject, sc)
    print(schema_id)


def producer_publish(scheduler, producer: KafkaProducerAvro):
    scheduler.enter(2, 1, producer_publish, (scheduler, producer))
    producer.publish(key='kkk', message={
        'k1': 'asdf',
        'k2': 'asdf2'
    })

cl_admin = ClusterAdmin(config['kafka.rest-proxy.url'])
cl_id = cl_admin.get_clusters()[0]      # what if there are no clusters!!
cl_admin.select_cluster(cl_id)          # assuming there are only one cluster
cl_admin.create_topic(KAFKA_TOPIC_NAME, 4, 1, True)

## Creating kafka producers
p = KafkaProducerAvro(KAFKA_TOPIC_NAME, 'test_topic-value')
sch = sched.scheduler(time.time, time.sleep)
sch.enter(2, 1, producer_publish, (sch, p))
sch.run()
