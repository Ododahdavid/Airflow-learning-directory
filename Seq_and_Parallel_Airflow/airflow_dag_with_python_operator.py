from airflow import DAG #pip3 install apache-airflow
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello_world_py(*args, **kwargs):
    print("Hello World from Python Operator... Ododah David you are doing great, keep it up...")
    
airflow_default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'email': ['davidododah40@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
# Creating our DAG object
dag = DAG(
    'DAG_For_Seq_Task', #DAG NAME
    default_args=airflow_default_args,  # these are args for the aifrlow dag, which you want to apply for all the tasks you will be defining within the DAG folder... our default args are defined above in the default_args variable
    description = 'First airflow DAG to understand sequencial tasks execution',
    start_date =datetime(2026, 2, 24), # when the DAG should start running, you can set it to a past date and time, and when you trigger the DAG, it will run all the tasks that are scheduled to run from the start date to the current date and time, but if you set it to a future date and time, the DAG will not run until that date and time is reached
    schedule_interval = "*/1 * * * *", # this is a cron expression that defines how often the DAg should run, in this case, it will run every minute, you can set it to a different cron expression to run it at a different interval, for example, if you want to run it every day at 8:00 AM, you can set it to "0 8 * * *", if you want to run it every hour, you can set it to "0 * * * *", if you want to run it every day at midnight, you can set it to "0 0 * * *", and so on...
    catchup=False, # this is a boolean that defines whether the DAG should catch up on missed runs, if set to True, the DAG will run all the missed runs from the start date to the current date and time, if set to False, the DAG will only run the latest run, which is the current date and time, in this case, we set it to False because we don't want to run all the missed runs, we just want to run the latest run when we trigger the DAG
    tags = ['dev']
)

task_1 = BashOperator(
    task_id = 'Print_From_Bash_Operator',
    bash_command = 'echo "Hello World from Bash Operator"',
    dag = dag
)

task_2 = PythonOperator(
    task_id = 'Print_From_Python_Operator',
    python_callable = hello_world_py,
    dag = dag
)

task_1 >> task_2 # this is how you define the sequence of tasks, in this case, task_1 will run first, and then task_2 will run after task_1 is completed, you can also use the set_downstream() method to define the sequence of tasks, for example, task_1.set_downstream(task_2) will achieve the same result as task_1 >> task_2, and you can also use the set_upstream() method to define the sequence of tasks, for example, task_2.set_upstream(task_1) will achieve the same result as task_1 >> task_2