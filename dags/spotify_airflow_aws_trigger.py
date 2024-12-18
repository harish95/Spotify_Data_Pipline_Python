from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta


default_args = {
    "owner":"harish",
    "depends_on_past":False,
    "start_date": datetime(2024,12,15)
    }

dag = DAG(
    dag_id="spotify_aws_trigger",
    default_args=default_args,
    description="Trigger to invoke lambda in AWS",
    schedule_interval=timedelta(days=1),
    catchup=False
)

trigger_spotify_data_extract = LambdaInvokeFunctionOperator(
    task_id = "extract_spotify_data",
    function_name="spotify_api_data_extract",
    aws_conn_id = "AWS_Connection",
    region_name = "ap-south-1",
    dag = dag
)

check_s3_upload = S3KeySensor(
    task_id = "Check_S3_Upload",
    bucket_key= "s3://hbx-spotify/raw/to_process/*",
    wildcard_match=True,
    aws_conn_id = "AWS_Connection",
    timeout = 60 * 60,
    poke_interval = 60,
    dag = dag
)


trigger_spotify_data_process = LambdaInvokeFunctionOperator(
    task_id = "process_spotify_data",
    function_name="Spotify_Transformation_Load",
    aws_conn_id = "AWS_Connection",
    region_name = "ap-south-1",
    dag = dag
)


trigger_spotify_data_extract >> check_s3_upload >> trigger_spotify_data_process



