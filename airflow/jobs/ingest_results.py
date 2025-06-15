import pyspark.sql.functions as F
import pyspark
from datetime import datetime
import os
from delta import * 

current_date = datetime.now()
current_month_partition = current_date.strftime("m=%Y-%m-01")

current_date = current_date.strftime("%Y-%m-%d")

DQOPS_HOME_DATA_PATH = '/opt/dqops/volume/.data/check_results'
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_ACCESS_KEY = 'password'
AWS_REGION = 'us-east-1'


conf = (
    pyspark.conf.SparkConf()
    .setAppName("MY_APP")
    .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .set("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .set("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_ACCESS_KEY)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.path.style.access", "true") 
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  
    .set("spark.hadoop.fs.s3a.connection.maximum", "100")
    .set("spark.sql.shuffle.partitions", "4")
    .setMaster("local[*]")
)

extra_packages = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.apache.hadoop:hadoop-common:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]


builder = pyspark.sql.SparkSession.builder.appName("MyApp").config(conf=conf)
spark = builder.getOrCreate()


def preprocess_check_results(file_path):
    df = spark.read.parquet(file_path)

    df = df.where(F.to_date('executed_at') == F.lit(current_date))

    df = df.withColumn('severity' , F.when(F.col('severity') == 0 , 'correct') \
                                .when(F.col('severity') == 1, 'warning') \
                                .when(F.col('severity') == 2, 'error') \
                                .otherwise('fatal')) 

    df.write \
        .mode("append") \
        .format("delta") \
        .save(f"s3a://dqops/check_results")

    return df

for c_dir in os.listdir(DQOPS_HOME_DATA_PATH):
    c_path = os.path.join(DQOPS_HOME_DATA_PATH, c_dir)

    for t_dir in os.listdir(c_path):
        t_path = os.path.join(c_path, t_dir)
        month_path = os.path.join(t_path, current_month_partition)

        if os.path.isdir(month_path):
            parquet_file_path = os.path.join(month_path, "check_results.0.parquet")
            preprocess_check_results(parquet_file_path)