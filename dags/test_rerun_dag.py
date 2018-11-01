from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timedelta

default_args = {
    'owner': 'amos',
    'depends_on_past': False,
    'start_date': datetime.now(),
}

dag = DAG('test_rerun_dag',
          default_args=default_args,
          description='test_rerun_dag',
          schedule_interval="*/1 * * * *")


primary_tasks = {}
secondary_tasks = {}
normal_tasks = {}

task_start = DummyOperator(task_id="task_start",dag=dag)
task_end = DummyOperator(task_id="task_end",dag=dag)
primary_start = DummyOperator(task_id="primary_start",dag=dag)
primary_end = DummyOperator(task_id="primary_end",dag=dag)
secondary_start = DummyOperator(task_id="secondary_start",dag=dag)
secondary_end = DummyOperator(task_id="secondary_end",dag=dag)
normal_start = DummyOperator(task_id="normal_start",dag=dag)
normal_end = DummyOperator(task_id="normal_end",dag=dag)


# template of primary_task
def gen_primary_task(id):
    name = "primary_" + str(id)
    primary_task = BashOperator(
        task_id=name,
        bash_command='echo ---- Primary task ---- & date +%Y-%m-%dT%H:%M:%s',
        # retries=3,
        dag=dag)
    primary_tasks[name] = primary_task

def gen_secondary_task(id):
    name = "secondary_" + str(id)
    secondary_task = BashOperator(
        task_id=name,
        bash_command='echo ---- Secondary task ---- & date +%Y-%m-%dT%H:%M:%s',
        # retries=3,
        dag=dag)
    secondary_tasks[name] = secondary_task

def gen_normal_task(id):
    name = "normal_" + str(id)
    normal_task = BashOperator(
        task_id=name,
        bash_command='echo ---- Normal task ---- & date +%Y-%m-%dT%H:%M:%s',
        # retries=3,
        dag=dag)
    normal_tasks[name] = normal_task


# generate tasks
for i in range(1,5):
    gen_primary_task(i)
    task_name = 'primary_' + str(i)
    primary_tasks[task_name].set_upstream(primary_start)
    primary_tasks[task_name].set_downstream(primary_end)

# generate tasks
for i in range(1,4):
    gen_secondary_task(i)
    task_name = 'secondary_' + str(i)
    secondary_tasks[task_name].set_upstream(secondary_start)
    secondary_tasks[task_name].set_downstream(secondary_end)

# generate tasks
for i in range(1,6):
    gen_normal_task(i)
    task_name = 'normal_' + str(i)
    normal_tasks[task_name].set_upstream(normal_start)
    normal_tasks[task_name].set_downstream(normal_end)

primary_start.set_upstream(task_start)
secondary_start.set_upstream(task_start)
normal_start.set_upstream(task_start)

primary_end.set_downstream(task_end)
secondary_end.set_downstream(task_end)
normal_end.set_downstream(task_end)

