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

# TODO: create a StructType for the Reservation schema for the following fields:
# {
#   "reservationId":"814840107",
#   "customerName":"Jim Harris", 
#   "truckNumber":"15867", 
#   "reservationDate":"Sep 29, 2020, 10:06:23 AM"
# }
reservationSchema = StructType(
    [
        StructField("reservationId", StringType()),
        StructField("customerName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("reservationDate", StringType()),
    ]
)

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("exercise2-reservation-base64").getOrCreate()

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
redis_server_df.withColumn("value",from_json("value",redisMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisData")

# TODO: using spark.sql, select key, zSetEntries[0].element as reservation from RedisData
entries_encoded_qry = spark.sql("select key, zSetEntries[0].element as reservation from RedisData")

# TODO: from the dataframe use the unbase64 function to select a column called reservation with the base64 decoded JSON, and cast it to a string
entries_encoded_df = entries_encoded_qry.withColumn("reservation", unbase64(entries_encoded_qry.reservation).cast("string"))

# TODO: using the customer location StructType, deserialize the JSON from the streaming dataframe, selecting column reservation.* as a temporary view called TruckReservation 
entries_encoded_df.withColumn("reservation", from_json("reservation", reservationSchema)) \
    .select(col('reservation.*')) \
    .createOrReplaceTempView("TruckReservation")

# TODO: using spark.sql select * from CustomerLocation
truck_reservation_qry = spark.sql("select * from TruckReservation where reservationDate is not null")

# TODO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
truck_reservation_qry \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

