from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="ingest_results_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    submit_spark = SparkSubmitOperator(
        task_id="inget_daily_results",
        application="/opt/airflow/jobs/ingest_results.py",  # Your Spark Python script
        conn_id="spark_default",  # Connection ID defined in Airflow UI (or use local)
        packages="io.delta:delta-spark_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        },
        deploy_mode="client", 
        application_args=[],
        retries=2, 
        verbose=True,
    )
