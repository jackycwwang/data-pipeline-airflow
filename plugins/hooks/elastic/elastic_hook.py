# Class used to define AirflowPlugin that is used to register custom plugin.
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook     # Abstract base class for hooks.

# 0. Import Elasticsearch class
from elasticsearch import Elasticsearch

# 1. Create a ES connection in Airflow


class ElasticHook(BaseHook):

    def __init__(self, conn_id="elastic_default", *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 2. Get the connection object "elastic_default" from Airflow metastore
        conn = self.get_connection(conn_id)

        # 3. Get credentials in order to initialize the ES connection
        conn_config = {}
        hosts = []
        if conn.host:
            hosts = conn.host.split(',')
        if conn.port:
            conn_config['port'] = int(conn.port)
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        # 4. Make the connection
        self.es = Elasticsearch(hosts, **conn_config)

        # 5. Specify the index
        self.index = conn.schema

    # It returns the informations about the es instance
    def info(self):
        return self.es.info()

    # Set the index we want to use in ES
    def set_index(self, index):
        self.index = index

    # Add a document to the index
    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res


# Register your plugin
class AirflowElasticPlugin(AirflowPlugin):
    name = 'elastic'
    hooks = [ElasticHook]
