from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

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

# TODO: create a StructType for the Customer schema for the following fields:
# {
#   "customerName":"Frank Aristotle",
#   "email":"Frank.Aristotle@test.com",
#   "phone":"7015551212",
#   "birthDay":"1948-01-01",
#   "accountNumber":"750271955",
#   "location":"Jordan"
# }
customerJSONSchema = StructType (
    [
        StructField("customerName",StringType()),
        StructField("email",StringType()),
        StructField("phone",StringType()),
        StructField("birthDay",StringType()),
        StructField("accountNumber",StringType()),
        StructField("location",StringType())        
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
spark = SparkSession.builder.appName("customer-record").getOrCreate()

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

# TODO: using spark.sql, select key, zSetEntries[0].element as redisEvent from RedisData
redis_server_qry = spark.sql("select key, zSetEntries[0].element as redisEvent from RedisData")

# TODO: from the dataframe use the unbase64 function to select a column called redisEvent with the base64 decoded JSON, and cast it to a string
redis_event_df_1 = redis_server_qry.withColumn("redisEvent", unbase64(redis_server_qry.redisEvent).cast("string"))

# TODO: repeat this a second time, so now you have two separate dataframes that contain redisEvent data
redis_event_df_2 = redis_server_qry.withColumn("redisEvent", unbase64(redis_server_qry.redisEvent).cast("string"))

# Filter df's for only those that contain the birthDay field (customer record)
redis_event_df_1.filter(col("redisEvent").contains("birthDay"))
redis_event_df_2.filter(~col("redisEvent").contains("birthDay"))

# TODO: using the customer StructType, deserialize the JSON from the first redis decoded streaming dataframe, selecting column customer.* as a temporary view called Customer 
redis_event_df_1 \
    .withColumn("customer", from_json("redisEvent", customerJSONSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("Customer")

# TODO: using the customer location StructType, deserialize the JSON from the second redis decoded streaming dataframe, selecting column customerLocation.* as a temporary view called CustomerLocation 
redis_event_df_2 \
    .withColumn("customerLocation", from_json("redisEvent", customerLocationSchema)) \
    .select(col('customerLocation.*')) \
    .createOrReplaceTempView("CustomerLocation")

# TODO: using spark.sql select accountNumber as customerAccountNumber, location as homeLocation, birthDay from Customer where birthDay is not null
customer_stream_qry = spark.sql("select accountNumber as customerAccountNumber, location as homeLocation, birthDay from Customer where birthDay is not null")

# TODO: select the customerAccountNumber, homeLocation, and birth year (using split)
customer_with_birthday = customer_stream_qry.select('customerAccountNumber','homeLocation',split(customer_stream_qry.birthDay,"-").getItem(0).alias("birthYear"))

# TODO: using spark.sql select accountNumber as locationAccountNumber, and location
customer_location_stream_qry = spark.sql("select accountNumber as locationAccountNumber, location from CustomerLocation")

# TODO: join the customer and customer location data using the expression: customerAccountNumber = locationAccountNumber
customers_qry = customer_location_stream_qry.join(customer_with_birthday, expr( """
   customerAccountNumber = locationAccountNumber
"""
))

# TODO: write the stream to the console, and configure it to run indefinitely
# can you find the customer(s) who are traveling out of their home countries?
# When calling the customer, customer service will use their birth year to help
# establish their identity, to reduce the risk of fraudulent transactions.
# +---------------------+-----------+---------------------+------------+---------+
# |locationAccountNumber|   location|customerAccountNumber|homeLocation|birthYear|
# +---------------------+-----------+---------------------+------------+---------+
# |            982019843|  Australia|            982019843|   Australia|     1943|
# |            581813546|Phillipines|            581813546| Phillipines|     1939|
# |            202338628|Phillipines|            202338628|       China|     1944|
# |             33621529|     Mexico|             33621529|      Mexico|     1941|
# |            266358287|     Canada|            266358287|      Uganda|     1946|
# |            738844826|      Egypt|            738844826|       Egypt|     1947|
# |            128705687|    Ukraine|            128705687|      France|     1964|
# |            527665995|   DR Congo|            527665995|    DR Congo|     1942|
# |            277678857|  Indonesia|            277678857|   Indonesia|     1937|
# |            402412203|   DR Congo|            402412203|    DR Congo|     1945|
# +---------------------+-----------+---------------------+------------+---------+
customers_qry \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

