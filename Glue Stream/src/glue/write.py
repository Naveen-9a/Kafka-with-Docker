from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType

spark = SparkSession.builder \
    .appName("Stream Player Data") \
    .getOrCreate()


data_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("Section", StringType(), True),
    StructField("Mar 2013", DoubleType(), True),
    StructField("Mar 2014", DoubleType(), True),
    StructField("Mar 2015", DoubleType(), True),
    StructField("Mar 2016", DoubleType(), True),
    StructField("Mar 2017", DoubleType(), True),
    StructField("Mar 2018", DoubleType(), True),
    StructField("Mar 2019", DoubleType(), True),
    StructField("Mar 2020", DoubleType(), True),
    StructField("Mar 2021", DoubleType(), True),
    StructField("Mar 2022", DoubleType(), True),
    StructField("Mar 2023", DoubleType(), True),
    StructField("Mar 2024", DoubleType(), True),
    StructField("diff_12_year", StringType(), True)
])

psql_jdbc_properties = {
    "user": "docker",
    "password": "docker",
    "driver": "org.postgresql.Driver"
}



def write_to_new_psql(batch_df, batch_id):
    try:
        print(f"Batch ID: {batch_id}")
        batch_df.show()  
        batch_df.write.jdbc(
            url="jdbc:postgresql://host.docker.internal:5432/reliance",
            table="sink_table",
            mode="append",
            properties=psql_jdbc_properties
        )
        print(f"Batch {batch_id} written to PostgreSQL successfully.")
    except Exception as e:
        print(f"Error writing batch {batch_id} to PostgreSQL: {e}")

df2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "new_topic") \
    .option("startingOffsets", "earliest") \
    .load()


raw_df2 = df2.selectExpr("CAST(value AS STRING) as json_value")


parsed_df2 = raw_df2.select(from_json(col("json_value"), data_schema).alias("data")) \
    .select("data.*")


print("Printing parsed data from new Kafka topic:")
query5 = parsed_df2.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


query6 = parsed_df2.writeStream \
    .foreachBatch(write_to_new_psql) \
    .start()


spark.streams.awaitAnyTermination()
