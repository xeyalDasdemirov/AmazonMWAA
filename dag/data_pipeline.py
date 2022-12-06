from datetime import timedelta  
import airflow  
from airflow import DAG  
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator 
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator

# Custom Operators deployed as Airflow plugins
from awsairflowlib.operators.aws_copy_s3_to_redshift import CopyS3ToRedshiftOperator

S3_BUCKET_NAME = "airflow-khayal-bucket"  
  
default_args = {  
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(  
    'data_pipeline',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)

s3_sensor = S3PrefixSensor(  
  task_id='s3_sensor',  
  bucket_name=S3_BUCKET_NAME,  
  prefix='data/input',  
  dag=dag  
)



execution_date = "{{ execution_date }}"  
  
JOB_FLOW_OVERRIDES = {
    "Name": "Data-Pipeline-" + execution_date,
    "ReleaseLabel": "emr-5.29.0",
    "LogUri": "s3://{}/logs/emr/".format(S3_BUCKET_NAME),
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2
            }
        ],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": True
    }
}

S3_URI = "s3://{}/scripts/emr/".format(S3_BUCKET_NAME)  
 
SPARK_TEST_STEPS = [
   {
       'Name': 'setup - copy files',
       'ActionOnFailure': 'CANCEL_AND_WAIT',
       'HadoopJarStep': {
           'Jar': 'command-runner.jar',
           'Args': ['aws', 's3', 'cp', '--recursive', S3_URI, '/home/hadoop/']
       }
   },
   {
       'Name': 'Run Spark',
       'ActionOnFailure': 'CANCEL_AND_WAIT',
       'HadoopJarStep': {
           'Jar': 'command-runner.jar',
           'Args': ['spark-submit',
                    '/home/hadoop/pl_aggregations.py',
                    's3://{}/data/input'.format(S3_BUCKET_NAME),
                    's3://{}/data/aggregated'.format(S3_BUCKET_NAME)]
       }
   }
]

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)


cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)


copy_agg_to_redshift = CopyS3ToRedshiftOperator(
    task_id='copy_to_redshift',
    schema='pl',
    table='soccer',
    s3_bucket=S3_BUCKET_NAME,
    s3_key='data/aggregated',
    iam_role_arn='xxxxxxxxxxxxxxxxxxx',
    copy_options=["FORMAT AS PARQUET"],
    dag=dag,
)

s3_sensor >> cluster_creator >> step_adder >> step_checker >> cluster_remover >> copy_agg_to_redshift


