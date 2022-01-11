from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StringType
import time
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, BooleanType, IntegerType, MapType, FloatType
##from elasticsearch import Elasticsearch
##from elasticsearch import helpers
sc = SparkContext.getOrCreate()

spark = SparkSession.builder.appName('DataStreamingApp').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "velib-stations") \
    .load()
query = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .start()
query.awaitTermination()

# Convert binary to string key and value
df1 = (df
       .withColumn("key", df["key"].cast(StringType()))
       .withColumn("value", df["value"].cast(StringType())))

print(df1)

# Event data schema
schema = StructType(
    [StructField("number", IntegerType(), True),
     StructField("contract_name", StringType(), True),
     StructField("name", StringType(), True),
     StructField("address", StringType(), True),
     StructField("position", MapType(FloatType(), FloatType(), True)),
     StructField("banking", BooleanType(), True),
     StructField("bonus", BooleanType(), True),
     StructField("bike_stands", IntegerType(), True),
     StructField("available_bike_stands", IntegerType(), True),
     StructField("available_bikes", IntegerType(), True),
     StructField("status", StringType(), True),
     StructField("last_update", IntegerType(), True)])

dff = (df1
       # Sets schema for event data

       .withColumn("value", from_json("value", schema))
       )

print(dff.select("value.number"))


dff_formatted = (dff.select(
    "value.number"
    , "value.contract_name"
    , "value.name"
    , "value.address"
    , "value.position"
    , "value.banking"
    , "value.bonus"
    , "value.bike_stands"
    , "value.available_bike_stands"
    , "value.available_bikes"
    , "value.status"
    , "value.last_update"
))
print(dff_formatted)

#write dataframe to elasticsearch
#df.write.format('').option("es.port", "9200").option("es.nodes" , "localhost").mode("append").save("userspark/doc")
#es = Elasticsearch()