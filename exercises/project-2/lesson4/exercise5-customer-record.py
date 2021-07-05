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

# TODO: using spark.sql, select key, zSetEntries[0].element as customer from RedisData
redis_server_qry = spark.sql("select key, zSetEntries[0].element as customer from RedisData")

# TODO: from the dataframe use the unbase64 function to select a column called customer with the base64 decoded JSON, and cast it to a string
customer_stream = redis_server_qry.withColumn("customer", unbase64(redis_server_qry.customer).cast("string"))

# TODO: using the customer StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a temporary view called Customer 
customer_stream \
    .withColumn("customer", from_json("customer", customerJSONSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("Customer")

# TODO: using spark.sql select accountNumber, location, birthDay from Customer where birthDay is not null
customer_qry_stream = spark.sql("select accountNumber, location, birthDay from Customer where birthDay is not null")

# TODO: select the account number, location, and birth year (using split)
customer_qry = customer_qry_stream.select('accountNumber','location',split(customer_qry_stream.birthDay,"-").getItem(0).alias("birthYear"))

# TODO: write the stream in JSON format to a kafka topic called customer-attributes, and configure it to run indefinitely, the console output will not show any output. 
#You will need to type /data/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic customer-attributes --from-beginning to see the JSON data
#
# The data will look like this: {"accountNumber":"288485115","location":"Brazil","birthYear":"1938"}
customer_qry.selectExpr("CAST(accountNumber AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "customer-attributes") \
    .option("checkpointLocation","/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()
