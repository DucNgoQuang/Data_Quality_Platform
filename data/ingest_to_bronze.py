import pyspark
import os
from dotenv import load_dotenv
from delta import * 

load_dotenv('.././.env')
access_key = os.getenv("API_KEY")
secret_key = os.getenv("SECRET_ACCESS_KEY")
print(access_key)


conf = (
    pyspark.conf.SparkConf()
    .setAppName("MY_APP")
    .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .set("spark.hadoop.fs.s3a.access.key", access_key)
    .set("spark.hadoop.fs.s3a.secret.key", secret_key)
    .set("spark.hadoop.fs.s3a.region", "ap-southeast-2")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.sql.shuffle.partitions", "4")
    .setMaster(
        "local[*]"
    )  
)

extra_packages = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.apache.hadoop:hadoop-common:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]

builder = pyspark.sql.SparkSession.builder.appName("MyApp").config(conf=conf)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=extra_packages
).getOrCreate()

df = spark.read.csv('nyc_menu.csv', header=True, inferSchema=True)

df.write.format("delta").save("s3a://vdt2025/nyc_menu_delta")