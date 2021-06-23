from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType, ArrayType, DateType

# TODO: create a kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}
schema_vehicle_status = StructType (
    [
        StructField("truckNumber", StringType()),
        StructField("destination", StringType()),
        StructField("milesFromShop", IntegerType()),
        StructField("odometerReading", IntegerType())           
    ]   
)

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("exercise1-vehicle-status").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: read the vehicle-status kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
vehicle_status_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","vehicle-status") \
    .option("startingOffsets","earliest") \
    .load()

# TODO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
vehicle_status_df = vehicle_status_stream.selectExpr("cast(key as string) key", "cast(value as string) value")

# TODO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TODO: create a temporary streaming view called "VehicleStatus" 
# it can later be queried with spark.sql
vehicle_status_df.withColumn("value",from_json("value",schema_vehicle_status)) \
        .select(col('value.*')) \
        .createOrReplaceTempView("VehicleStatus")

# TODO: using spark.sql, select * from VehicleStatus
vehicle_status_qry = spark.sql("select * from VehicleStatus")

# TODO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-----------+------------+-------------+---------------+
# |truckNumber| destination|milesFromShop|odometerReading|
# +-----------+------------+-------------+---------------+
# |       9974|   Tennessee|          221|         335048|
# |       3575|      Canada|          354|          74000|
# |       1444|      Nevada|          257|         395616|
# |       5540|South Dakota|          856|         176293|
# +-----------+------------+-------------+---------------+
vehicle_status_qry \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

