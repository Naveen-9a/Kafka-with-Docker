from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
   .appName("Stream Player Data") \
   .getOrCreate()
 
# Define the schema for the Kafka data
payload_after_schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("name", StringType(), True),
   StructField("postion", StringType(), True),
   StructField("payment", IntegerType(), True)
   
])
 
payload_schema = StructType([
   StructField("after", payload_after_schema, True)
])
 
message_schema = StructType([
   StructField("payload", payload_schema, True)
])
 
# Read data from Kafka
df1 = spark.readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers", "kafka:9092") \
   .option("subscribe", "postgres.public.player") \
   .option("includeHeaders", "true") \
   .load()
 
# Print raw Kafka data schema for debugging
print("Schema of raw Kafka data:")
df1.printSchema()
 
# Extract the JSON value from the Kafka message
raw_df = df1.selectExpr("CAST(value AS STRING) as json_value")
 
# Print the raw JSON value to understand the format
print("Printing raw JSON values from Kafka:")
query1 = raw_df.writeStream \
   .format("console") \
   .option("truncate", "false") \
   .start()
 
# Parse the JSON data using the defined schema
parsed_df = raw_df.select(from_json(col("json_value"), message_schema).alias("data")) \
   .select("data.payload.after.*")
 
# Print the parsed data to debug
print("Printing parsed data after applying schema:")
query2 = parsed_df.writeStream \
   .format("console") \
   .option("truncate", "false") \
   .start()
 
# Perform a simple transformation
transformed_df = parsed_df.withColumn("bouns",col("payment")*0.10 )
 
# Print transformed data to verify the transformation
print("Printing transformed data to verify the transformation:")
query3 = transformed_df.writeStream \
   .format("console") \
   .option("truncate", "false") \
   .option("topic","new_topic")\
   .start()
 
# Define PostgreSQL JDBC properties
# psql_jdbc_url = "jdbc:postgresql://postgres:5432/testdb"
# psql_jdbc_properties = {
#    "user": "postgres",
#    "password": "password",
#    "driver": "org.postgresql.Driver"
# }
 
# Function to write to PostgreSQL
# def write_to_psql(batch_df, batch_id):
#    try:
#        print(f"Batch ID: {batch_id}")
#        batch_df.show()  # Print the batch DataFrame for debugging
#        batch_df.write.jdbc(
#            url=psql_jdbc_url,
#            table="employee",
#            mode="append",
#            properties=psql_jdbc_properties
#        )
#        print(f"Batch {batch_id} written to PostgreSQL successfully.")
#    except Exception as e:
#        print(f"Error writing batch {batch_id} to PostgreSQL: {e}")
 
# Write the transformed data to PostgreSQL
# query4 = transformed_df.writeStream \
#    .foreachBatch(write_to_psql) \
#    .start()
 
# Await termination of all queries
spark.streams.awaitAnyTermination()
 
query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
# query4.awaitTermination()
 
 
spark.stop()