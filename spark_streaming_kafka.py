from pyspark.sql import SparkSession
import requests
from kafka import KafkaProducer
import json
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def init(spark):
    url = "https://api.openaq.org/v1/cities"
    authorization = "Bearer" + " " + "aTC66AHJcJGYTBxwrhT7IkyptwEL"  # self.access_token
    headers = {
        "Authorization": authorization,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

    result_dict = response.json()["results"]
    for message in result_dict:
        binary_data = json.dumps(message)
        producer.send("test", binary_data)


def get_full_data(spark):
    init(spark)

    schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("locations", IntegerType(), True),
            StructField("count", IntegerType(), True),
        ]
    )

    df_from_api = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json("json", schema).alias("data"))
    )

    df_from_api.printSchema()

    query = (
        df_from_api.writeStream.format("parquet")
        .option("path", "/tmp/pydata")
        .option("checkpointLocation", "/tmp/checkpoint")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    warehouse_location = "file:${system:user.dir}/spark-warehouse"
    spark = (
        SparkSession.builder.appName("Spark Streaming")
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.sql.streaming.checkpointLocation", "/tmp/sparkcheckpoint")
        .getOrCreate()
    )

    get_full_data(spark)
