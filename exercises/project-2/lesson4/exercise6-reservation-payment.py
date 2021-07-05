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

# TODO: create a StructType for the Reservation schema for the following fields:
# {
#   "reservationId":"814840107",
#   "customerName":"Jim Harris", 
#   "truckNumber":"15867", 
#   "reservationDate":"Sep 29, 2020, 10:06:23 AM"
# }
reservationJSONSchema = StructType (
    [
        StructField("reservationId", StringType()),
        StructField("customerName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("reservationDate", StringType()),
    ]   
)

# TODO: create a StructType for the Payment schema for the following fields:
# {
#   "reservationId":"9856743232",
#   "customerName":"Frank Aristotle",
#   "date":"Sep 29, 2020, 10:06:23 AM",
#   "amount":"946.88"
# }
paymentJSONSchema = StructType (
    [
        StructField("reservationId",StringType()),
        StructField("customerName",StringType()),
        StructField("date",StringType()),
        StructField("amount",StringType())
    ]
)

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("reservation-payment").getOrCreate()

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

# TODO: using the reservation StructType, deserialize the JSON from the first redis decoded streaming dataframe, selecting column reservation.* as a temporary view called Reservation 
redis_event_df_1.filter(col("redisEvent").contains("reservationDate"))

# TODO: using the payment StructType, deserialize the JSON from the second redis decoded streaming dataframe, selecting column payment.* as a temporary view called Payment 
redis_event_df_2.filter(~(col("redisEvent").contains("reservationDate")))

# TODO: using spark.sql select select reservationId, reservationDate from Reservation where reservationDate is not null
redis_event_df_1 \
    .withColumn("reservation", from_json("redisEvent", reservationJSONSchema)) \
    .select(col('reservation.*')) \
    .createOrReplaceTempView("Reservation")

reservation_df = spark.sql("select reservationId, reservationDate from Reservation where reservationDate is not null")

# TODO: using spark.sql select reservationId as paymentReservationId, date as paymentDate, amount as paymentAmount from Payment
redis_event_df_2\
    .withColumn("payment", from_json("redisEvent", paymentJSONSchema)) \
    .select(col('payment.*')) \
    .createOrReplaceTempView("Payment")

payment_df = spark.sql("select reservationId as paymentReservationId, amount as paymentAmount from Payment")

# TODO: join the reservation and payment data using the expression: reservationId=paymentReservationId
reservation_payment_df = reservation_df.join(payment_df, expr("""reservationId = paymentReservationId"""))

# TODO: write the stream to the console, and configure it to run indefinitely
# can you find the reservations who haven't made a payment on their reservation?
reservation_payment_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

