from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType

# TODO: create a Vehicle Status kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}
vehicleStatusSchema = StructType (
    [
        StructField("truckNumber", StringType()),
        StructField("destination", StringType()),
        StructField("milesFromShop", IntegerType()),
        StructField("odometerReading", IntegerType())     
    ]   
)

# TODO: create a Checkin Status kafka message schema StructType including the following JSON elements:
# {"reservationId":"1601485848310","locationName":"New Mexico","truckNumber":"3944","status":"In"}
vehicleCheckinSchema = StructType (
    [
        StructField("reservationId", StringType()),
        StructField("locationName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("status", StringType())     
    ]
)

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("vehicle-checkin").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
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
# TODO: create a temporary streaming view called "BankDeposits" 
# it can later be queried with spark.sql
vehicle_status_df \
        .withColumn("value",from_json("value",vehicleStatusSchema)) \
        .select(col('value.*')) \
        .createOrReplaceTempView("VehicleStatus")

# TODO: using spark.sql, select truckNumber as statusTruckNumber, destination, milesFromShop, odometerReading from VehicleStatus into a dataframe
vehicle_status_qry = spark.sql("select truckNumber as statusTruckNumber, destination, milesFromShop, odometerReading from VehicleStatus")

# TODO: read the bank-customers kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
vehicle_checkin_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","check-in") \
    .option("startingOffsets","earliest") \
    .load()

# TODO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
vehicle_checkin_df = vehicle_checkin_stream.selectExpr("cast(key as string) key", "cast(value as string) value")

# TODO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TODO: create a temporary streaming view called "BankCustomers" 
# it can later be queried with spark.sql
vehicle_checkin_df \
    .withColumn("value",from_json("value",vehicleCheckinSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("VehicleCheckin")

# TODO: using spark.sql, select reservationId, locationName, truckNumber as checkinTruckNumber, status from VehicleCheckin into a dataframe
vehicle_checkin_qry = spark.sql("select reservationId, locationName, truckNumber as checkinTruckNumber, status from VehicleCheckin")

# TODO: join the customer dataframe with the deposit dataframe
join_expr = """statusTruckNumber = checkinTruckNumber"""
checkinStatusDF = vehicle_status_qry.join(vehicle_checkin_qry, expr(join_expr))

# TODO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+
# |statusTruckNumber| destination|milesFromShop|odometerReading|reservationId|locationName|checkinTruckNumber|status|
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+
# |             1445|Pennsylvania|          447|         297465|1602364379489|    Michigan|              1445|    In|
# |             1445|     Colardo|          439|         298038|1602364379489|    Michigan|              1445|    In|
# |             1445|    Maryland|          439|         298094|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          439|         298185|1602364379489|    Michigan|              1445|    In|
# |             1445|    Maryland|          439|         298234|1602364379489|    Michigan|              1445|    In|
# |             1445|      Nevada|          438|         298288|1602364379489|    Michigan|              1445|    In|
# |             1445|   Louisiana|          438|         298369|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          438|         298420|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          436|         298471|1602364379489|    Michigan|              1445|    In|
# |             1445|  New Mexico|          436|         298473|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          434|         298492|1602364379489|    Michigan|              1445|    In|
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+
checkinStatusDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

