
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
    def extractDataOld(key):
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        redis_cn = redis.Redis(host= '10.233.49.128',    port= '6379',    password="tmCN3FwkP7")
        response = redis_cn.get(key)
        response = json.loads(response)
        df = pd.DataFrame(response)
        df = df[df.columns].add_prefix('old_')
        return df

    # [END extract]

    # [START main_flow]
    key_process = str(config["platform_id"])+"-"+str( config["platform_name"])
    print("---------------- OLD ---------")
    order_data = extractDataOld(key_process)
    # [END main_flow]


# [START dag_invocation]
puller = puller()
# [END dag_invocation]

# [END tutorial]
