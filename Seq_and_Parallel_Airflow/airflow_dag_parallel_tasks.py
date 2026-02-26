from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'email': ['davidododah40@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'DAG_For_parallel_Task', #DAG NAME
    default_args= default_args,  
    description = 'Demo to show how parallel tasks get executed',
    start_date =datetime(2026, 2, 25),
    schedule_interval = "*/2 * * * *", 
    catchup=False, 
    tags = ['dev']
)

start_task = BashOperator(task_id = 'Start_Task', bash_command = 'echo "Start task"', dag=dag)

parallel_task_1 = BashOperator(task_id = 'Parallel_Task_1', bash_command = 'echo "Parallel Task 1"', dag=dag)
parallel_task_2 = BashOperator(task_id = 'Parallel_Task_2', bash_command = 'echo "Parallel Task 2"', dag=dag)
parallel_task_3 = BashOperator(task_id = 'Parallel_Task_3', bash_command = 'echo "Parallel Task 3"', dag=dag)

end_task = BashOperator(task_id = 'End_Task', bash_command = 'echo "End task"', dag=dag)

start_task >> [parallel_task_1, parallel_task_2, parallel_task_3] >> end_task # this is how you define parallel tasks, in this case, parallel_task_1, parallel_task_2, and parallel_task_3 will run in parallel after start_task is completed, and then end_task will run after all the parallel tasks are completed 