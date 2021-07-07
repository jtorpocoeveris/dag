
# pylint: disable=missing-function-docstring
# [START]
# [START import_module]
import json
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import redis
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

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
        df = pd.DataFrame(response)
        df = df[df.columns].add_prefix('old_')
        return df

    # [END extract]

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
    key_process = str(config["platform_id"])+"-"+str(config["platform_name"])
    order_data = extract_old(key_process)
    # [END main_flow]


# [START dag_invocation]
puller = puller()
# [END dag_invocation]

# [END tutorial]
