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
redisServerRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","redis-server") \
    .option("startingOffsets","earliest") \
    .load()

# TODO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them

# TODO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 

# TODO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql

# TODO: using spark.sql, select key, zSetEntries[0].element as reservation from RedisData

# TODO: from the dataframe use the unbase64 function to select a column called reservation with the base64 decoded JSON, and cast it to a string

# TODO: using the customer location StructType, deserialize the JSON from the streaming dataframe, selecting column reservation.* as a temporary view called TruckReservation 

# TODO: using spark.sql select * from CustomerLocation

# TODO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:



