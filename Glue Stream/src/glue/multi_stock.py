from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,to_timestamp
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
# import psycopg2

spark = SparkSession.builder \
    .appName("ETL Profit and Loss") \
    .getOrCreate()
 

profit_loss_schema =profit_loss_schema = StructType([
    StructField("change_id", IntegerType(), False), 
    StructField("operation", StringType(), True),      
    StructField("report_date", StringType(), True),  
    StructField("sales", IntegerType(), True),  
    StructField("raw_material_cost", IntegerType(), True),  
    StructField("change_in_inventory", IntegerType(), True),  
    StructField("power_and_fuel", IntegerType(), True),  
    StructField("other_mfr_exp", IntegerType(), True),  
    StructField("employee_cost", IntegerType(), True),  
    StructField("selling_and_admin", IntegerType(), True),  
    StructField("other_expenses", IntegerType(), True),  
    StructField("other_income", IntegerType(), True),  
    StructField("depreciation", IntegerType(), True),  
    StructField("interest", IntegerType(), True),  
    StructField("profit_before_tax", IntegerType(), True),  
    StructField("tax", IntegerType(), True),  
    StructField("net_profit", IntegerType(), True),  
    StructField("dividend_amount", IntegerType(), True),  
    StructField("stock", StringType(), True) 
])
 
payload_schema = StructType([
    StructField("after", profit_loss_schema, True)
])
 

message_schema = StructType([
    StructField("payload", payload_schema, True)
])
 

df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "rel.public.profit_loss_data_sel_changes") \
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

parsed_df = parsed_df.withColumn("report_date", to_timestamp(col("report_date"), "yyyy-MM-dd HH:mm:ss"))

transformed_df = parsed_df.withColumn("operating_profit",col("sales")-(col("raw_material_cost")+ col('power_and_fuel')\
                                                                       +col('other_mfr_exp')+col('employee_cost')\
                                                                        +col('selling_and_admin')+col('other_expenses')-1*col('change_in_inventory')))
 
query2 = transformed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


##############################################################################################################################
jdbc_url = "jdbc:postgresql://host.docker.internal:5432/reliance"
connection_properties = {
    "user": "docker",
    "password": "docker",
    "driver": "org.postgresql.Driver"
}


def write_to_postgres(df, epoch_id):
    inserts = df.filter(df.operation == "INSERT")
    # updates = df.filter(df.operation == "UPDATE")
    # deletes = df.filter(df.operation == "DELETE")

    if not inserts.rdd.isEmpty():
        inserts.write.jdbc(url=jdbc_url, table="sink_profit_loss", mode="append", properties=connection_properties)

    # if not updates.rdd.isEmpty():
    #     conn = psycopg2.connect(
    #         dbname="reliance",
    #         user="docker",
    #         password="docker",
    #         host="192.168.1.188",
    #         port="5432"
    #     )
    #     cursor = conn.cursor()
    #     index = updates.select("index")
    #     index_values = index.collect()
    #     index_tuple = tuple([row["index"] for row in index_values])
    #     cursor.execute(f"alter table sink_profit_loss replica identity full;")
    #     if len(index_tuple) == 1:
    #         cursor.execute(f"delete from sink_profit_loss where index = {index_tuple[0]}")
    #     else:    
    #         cursor.execute(f"DELETE FROM sink_profit_loss WHERE index in {index_tuple}")
    #     conn.commit()
    #     cursor.close()
    #     conn.close()

    #     updates.write.jdbc(url=jdbc_url, table="sink_profit_loss", mode="append", properties=connection_properties)


    # if not deletes.rdd.isEmpty():

    #     conn = psycopg2.connect(
    #         dbname="reliance",
    #         user="docker",
    #         password="docker",
    #         host="192.168.1.188",
    #         port="5432"
    #     )
    #     cursor = conn.cursor()
    #     index = deletes.select("index")
    #     index_values = index.collect()
    #     index_tuple = tuple([row["index"] for row in index_values])
    #     cursor.execute(f"alter table sink_profit_loss replica identity full;")
    #     if len(index_tuple) == 1:
    #         cursor.execute(f"delete from sink_profit_loss where index = {index_tuple[0]};")
    #     else:    
    #         cursor.execute(f"DELETE FROM sink_profit_loss WHERE index in {index_tuple};")
    #     conn.commit()
    #     cursor.close()
    #     conn.close()
            




#####################################################################################################################
df = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13),
    ],
    ["first_name", "age"],
)

df.show()

query = transformed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

spark.streams.awaitAnyTermination()