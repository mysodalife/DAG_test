from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash_operator import BaseOperator
from airflow.operators.python_operator import BranchPythonOperator,PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.redis_hook import RedisHook
default_arg = {
    "owner":"airflow",   # owner
    "depends_on_past":False, # 是否依赖于上次执行的task
    "start_date":datetime(2020,11,1),
    "email": "404997294@qq.com",
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    "end_date":datetime(2020,11,11),
    "execution_timeout":timedelta(seconds=300),
}
dag = DAG(dag_id="test-dag", description="this is test demo",default_args=default_arg,schedule_interval=None)
get_timestamp = BaseOperator(task_id="get_timestamp", bash_command='date + %s',xcom_push=True,dag=dag)
branching = BranchPythonOperator(task_id="branching", python_callable= lambda **context:'store_in_redis' if int(context['task_instance'].xcom_pull(task_ids='get_timestamp')) % 2 == 0 else 'skip', provide_context=True,dag=dag)

def set_last_timestamp_in_redis(**context):
    timestamp = context['task_instance'].xcom_pull(task_ids='get_timestamp')
    redis = RedisHook(redis_conn_id='redis_default',).get_conn()
    redis.set('last_timestamp', timestamp)
store_in_redis =  PythonOperator(task_id='store_in_redis',python_callable=set_last_timestamp_in_redis,provide_context=True,dag=dag)
skip = DummyOperator(task_id='skip', dag=dag)
get_timestamp >> branching >> [store_in_redis, skip]
