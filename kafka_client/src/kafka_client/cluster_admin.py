import requests, json

class InvalidClusterException(Exception):
    pass

class TopicException(Exception):
    pass

class ClusterAdmin:
    def __init__(self, kafka_rest_url: str):
        self._sess = requests.Session()
        self._curr_cluster = None
        self._kafka_base_url = kafka_rest_url

    def _handle_connection_exc(self, e):
        print(e, flush=True)

    def get_clusters(self) -> list:
        resp = self._sess.get(f'{self._kafka_base_url}/v3/clusters').json()
        if 'data' in resp.keys():
            return [d['cluster_id'] for d in resp['data']]
        
    def select_cluster(self, cluster_id: str):
        valid_clusters = self.get_clusters()
        if cluster_id in valid_clusters:
            self._curr_cluster = cluster_id
        else:
            raise InvalidClusterException(f'cluster with id {cluster_id} is not valid. you can fetch existing clusters by using `.get_clusters()` method')

    def check_cluster(self):
        return self._curr_cluster != None

    def get_topics(self):
        if not self.check_cluster():
            raise InvalidClusterException('selected cluster cannot be None')
        resp = self._sess.get(f'{self._kafka_base_url}/v3/clusters/{self._curr_cluster}/topics').json()
        if 'data' in resp.keys():
            return [d['topic_name'] for d in resp['data']]


    def check_topic(self, topic_name: str):
        topics = self.get_topics()
        return topic_name in topics

    def create_topic(self, topic_name: str, n_partitions: int, n_replications: int, ignore_if_exists: bool = True):
        if not self.check_cluster():
            raise InvalidClusterException('selected cluster cannot be None')
        if self.check_topic(topic_name) and not ignore_if_exists:
            raise TopicException(f'Topic {topic_name} already exists in cluster with id {self._curr_cluster}')
        if self.check_topic(topic_name) and ignore_if_exists:
            return topic_name
        
        body = json.dumps({
            'topic_name': topic_name,
            'partitions_count': n_partitions,
            'replication_factor': n_replications
        }).encode('utf-8')
        resp = self._sess.post(f'{self._kafka_base_url}/v3/clusters/{self._curr_cluster}/topics',
                        data=body, headers={'Content-Type': 'application/json'}).json()
        if not 'topic_name' in resp:
            raise TopicException(f'Error while creating topic {topic_name} in cluster {self._curr_cluster}')
        return topic_name














