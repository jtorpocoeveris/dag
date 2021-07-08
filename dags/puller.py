
# pylint: disable=missing-function-docstring
# [START]
# [START import_module]
import json
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import cross_downstream
from airflow.operators.dummy import DummyOperator
import redis
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule
# config = open("config.json","r")
# config = json.loads(config.read())
# config = config[0]

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
# [END default_args]


# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['idirect_lima'])
def puller():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract_old(key):
        redis_cn = redis.Redis(host= '10.233.49.128',    port= '6379',    password="tmCN3FwkP7")
        response = redis_cn.get(key)
        response = json.loads(response)
        # df = pd.DataFrame(response)
        # df = df[df.columns].add_prefix('old_')
        # return df
        return response

    @task()
    def extract_platform(key):
        # df = pd.DataFrame(response)
        # df = df[df.columns].add_prefix('old_')
        # return df
        return ['ok']

    @task()
    def extract_mongo(key):
        # df = pd.DataFrame(response)
        # df = df[df.columns].add_prefix('old_')
        # return df
        return ['ok']

    @task()
    def extract_mysql(key):
        # df = pd.DataFrame(response)
        # df = df[df.columns].add_prefix('old_')
        # return df
        return ['ok']


    @task()
    def comparate_old_vs_new():
        # df = pd.DataFrame(response)
        # df = df[df.columns].add_prefix('old_')
        # return df
        return ['ok']

    # [END extract]

    # [START load]
    @task()
    def load(data):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(data)

    # [END load]
    # [START main_flow]
    
    config = [
      {
        "route_trunk": "data",
        "url": "http://192.168.36.50:81/api/v1/evo/config/obj/remote",
        "user": "systemapi",
        "password": "tiws2019",
        "timeout": 120,
        "verify": "False",
        "platform_id": 2,
        "mysql_table": "bifrost_terminal_test",
        "mongo_normalization": "puller",
        "mongo_limit_time": 55,
        "mongo_collection": "idirect_test_lima",
        "primary_join_cols": {
          "mysql": "siteId",
          "mongo": "siteId",
          "platform": "Name",
          "old": "Name"
        },
        "secondary_join_cols": {
          "mysql": [
            "mysql_siteId",
            "mysql_id_nms"
          ],
          "mongo": [
            "mongo_Name",
            "mongo_ID"
          ],
          "platform": [
            "platform_Name",
            "platform_ID"
          ],
          "old": [
            "old_Name",
            "old_ID"
          ]
        },
        "platform_name": "idirect_lima"
      }
    ]
    key_process = str(config[0]["platform_id"])+"-"+str(config[0]["platform_name"])

    platform_data = extract_platform(key_process)
    old_data = extract_old(key_process)
    mongo_data = extract_mongo(key_process)
    mysql_data = extract_mysql(key_process)
    old_vs_new = comparate_old_vs_new()
    

    plat = DummyOperator(task_id="platform_data")
    old = DummyOperator(task_id="old_data")
    mongo = DummyOperator(task_id="mongo_data")
    mysql = DummyOperator(task_id="mysql_data")
    plat_old = DummyOperator(task_id="old_vs_new")
    # load(order_data,platform_data)
    [plat,old] >> plat_old
    mongo
    mysql
    # [END main_flow]


# [START dag_invocation]
puller = puller()
# [END dag_invocation]

# [END tutorial]
