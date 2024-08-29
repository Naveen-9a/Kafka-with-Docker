import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType,DoubleType
from pyspark.sql.functions import concat, lit
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Stream Player Data") \
    .getOrCreate()

# Define the schema for the Kafka data
payload_after_schema = StructType([
    StructField("index", LongType(), True),
    StructField("Year", StringType(), True),
    StructField("Salesￂﾠ+", LongType(), True),
    StructField("Expensesￂﾠ+", LongType(), True),
    StructField("Operating Profit", LongType(), True),
    StructField("OPM %", LongType(), True),
    StructField("Other Incomeￂﾠ+", LongType(), True),
    StructField("Interest", LongType(), True),
    StructField("Depreciation", LongType(), True),
    StructField("Profit before tax", LongType(), True),
    StructField("Tax %", LongType(), True),
    StructField("Net Profitￂﾠ+", LongType(), True),
    StructField("EPS in Rs", DoubleType(), True),
    StructField("Stock", StringType(), True),
])  

payload_schema = StructType([
    StructField("after", payload_after_schema, True)
])


message_schema = StructType([
    StructField("payload", payload_schema, True)
])


df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "rel.public.profit_loss_data") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()

print("Schema of raw Kafka data:")
df1.printSchema()


raw_df = df1.selectExpr("CAST(value AS STRING) as json_value")


print("Printing raw JSON values from Kafka:")
query1 = raw_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


parsed_df = raw_df.select(from_json(col("json_value"), message_schema).alias("data")) \
    .select("data.payload.after.*")


print("Printing parsed data after applying schema:")
query2 = parsed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


transformed_df = parsed_df.withColumn("Com_Key",concat(lit('Rel '),col("Year")))


print("Printing transformed data to verify the transformation:")
query3 = transformed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


query4 = transformed_df.selectExpr( "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "new_topic") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

spark.streams.awaitAnyTermination()

