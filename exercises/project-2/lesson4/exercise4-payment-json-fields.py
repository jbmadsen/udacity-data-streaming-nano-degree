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

# TODO: create a StructType for the Payment schema for the following fields:
# {"reservationId":"9856743232","customerName":"Frank Aristotle","date":"Sep 29, 2020, 10:06:23 AM","amount":"946.88"}
paymentJSONSchema = StructType (
    [
        StructField("reservationId",StringType()),
        StructField("customerName",StringType()),
        StructField("date",StringType()),
        StructField("amount",StringType())
    ]
)

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("payment-json").getOrCreate()

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

# TODO: using spark.sql, select key, zSetEntries[0].element as payment from RedisData
payment_qry_stream = spark.sql("select key, zSetEntries[0].element as payment from RedisData")

# TODO: from the dataframe use the unbase64 function to select a column called payment with the base64 decoded JSON, and cast it to a string
payment_qry_df = payment_qry_stream.withColumn("payment", unbase64(payment_qry_stream.payment).cast("string"))

# TODO: using the payment StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a temporary view called Customer 
payment_qry_df \
    .withColumn("payment", from_json("payment", paymentJSONSchema)) \
    .select(col('payment.*')) \
    .createOrReplaceTempView("Payment")

# TODO: using spark.sql select reservationId, amount,  from Payment
payment_qry = spark.sql("select reservationId, amount from Payment")

# TODO: write the stream in JSON format to a kafka topic called payment-json, and configure it to run indefinitely, the console output will not show any output. 
#You will need to type /data/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic customer-attributes --from-beginning to see the JSON data
#
# The data will look like this: {"reservationId":"9856743232","amount":"946.88"}
payment_qry.selectExpr("CAST(reservationId AS STRING) AS key", "to_json(struct(*)) AS value")\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "payment-json") \
    .option("checkpointLocation","/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()

