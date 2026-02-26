from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# IDs
CLUSTER_NAME = "dataproc-spark-airflow-demo-2"
PROJECT_ID = "keen-surfer-472111-m7"
REGION = "us-east1"
ZONE = "us-east1-c"           # ✅ zone

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 2, 25),
}

with DAG(
    dag_id="Employee_data_analysis_with_spark",
    default_args=default_args,
    description="Create Dataproc cluster, run PySpark job, delete cluster",
    schedule_interval=None,
    catchup=False,
    tags=["dev"],
) as dag:

    CLUSTER_CONFIG = {
        "gce_cluster_config": {
            "zone_uri": ZONE,   # ✅ pick a zone with capacity
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
        },
        "worker_config": {
            "num_instances": 0,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
        },
        "software_config": {
            "image_version": "2.2.26-debian12",
        },
    }

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    PYSPARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://test-demo-ododah/airflow-project-1/spark-job/emp_batch_job.py"
        },
    }

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job_on_dataproc",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",
    )

    create_cluster >> submit_pyspark_job >> delete_cluster