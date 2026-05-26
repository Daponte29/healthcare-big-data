from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator

# You will map these through Airflow Variables or environment variables in your local docker setup
EMR_SERVERLESS_APP_ID = "YOUR_EMR_SERVERLESS_APP_ID"
EXECUTION_ROLE_ARN = "YOUR_AWS_EXECUTION_ROLE_ARN"
S3_SCRIPT_URI = "s3://your-healthcare-bucket/scripts/main.py"
S3_DATA_URI = "s3://your-healthcare-bucket/data/"
S3_OUTPUT_URI = "s3://your-healthcare-bucket/outputs/clustering/"
S3_LOGS_URI = "s3://your-healthcare-bucket/logs/"

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'healthcare_clustering_emr_serverless',
    default_args=default_args,
    description='Triggers clustering big data pipeline on EMR Serverless',
    schedule_interval=None, # Trigger manually or via event
    start_date=days_ago(1),
    tags=['healthcare', 'spark', 'emr-serverless'],
) as dag:

    run_clustering_job = EmrServerlessStartJobRunOperator(
        task_id="run_spark_clustering",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": S3_SCRIPT_URI,
                "entryPointArguments": [
                    "--data-uri", S3_DATA_URI,
                    "--output-uri", S3_OUTPUT_URI
                ],
                "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=16g --conf spark.driver.cores=2 --conf spark.driver.memory=8g --py-files s3://your-healthcare-bucket/scripts/src.zip"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": S3_LOGS_URI
                }
            }
        },
        name="healthcare-clustering-job",
    )

    run_clustering_job
