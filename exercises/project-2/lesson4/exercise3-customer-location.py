from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

# TODO: create a StructType for the CustomerLocation schema for the following fields:
# {"accountNumber":"814840107","location":"France"}
customerLocationSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType()),
    ]   
)

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("customer-location").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
redis_server_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","redis-server") \
    .option("startingOffsets","earliest") \
    .load()   

# TODO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
redis_server_df = redis_server_stream.selectExpr("cast(key as string) key", "cast(value as string) value")

# TODO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 
# TODO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
redis_server_df.withColumn("value",from_json("value",redisMessageSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("RedisData")

# TODO: using spark.sql, select key, zSetEntries[0].element as customerLocation from RedisData
customer_location_qry_stream = spark.sql("select key, zSetEntries[0].element as customerLocation from RedisData")

# TODO: from the dataframe use the unbase64 function to select a column called customerLocation with the base64 decoded JSON, and cast it to a string
customer_location_df = customer_location_qry_stream.withColumn("customerLocation", unbase64(customer_location_qry.customerLocation).cast("string"))

# TODO: using the customer location StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a temporary view called CustomerLocation 
customer_location_df \
    .withColumn("customerLocation", from_json("customerLocation", customerLocationSchema)) \
    .select(col('customerLocation.*')) \
    .createOrReplaceTempView("CustomerLocation")

# TODO: using spark.sql select * from CustomerLocation
customer_location_qry = spark.sql("select * from CustomerLocation")

# TODO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-------------+---------+
# |accountNumber| location|
# +-------------+---------+
# |         null|     null|
# |     93618942|  Nigeria|
# |     55324832|   Canada|
# |     81128888|    Ghana|
# |    440861314|  Alabama|
# |    287931751|  Georgia|
# |    413752943|     Togo|
# |     93618942|Argentina|
# +-------------+---------+
customer_location_qry \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

