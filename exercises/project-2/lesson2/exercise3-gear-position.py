from pyspark.sql import SparkSession

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("exercise3-gear-position").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: read the gear-position kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
df_gear_position_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","gear-position") \
    .option("startingOffsets","earliest") \
    .load()

# TODO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
df_gear_position = df_gear_position_stream.selectExpr("cast(key as string) truckId", "cast(value as string) gearPosition")

# TODO: create a temporary streaming view called "GearPosition" based on the streaming dataframe
df_gear_position.createOrReplaceTempView("GearPosition")

# TODO: query the temporary view "GearPosition" using spark.sql 
qry_gear_position = spark.sql("select * from GearPosition")

# TODO: Write the dataframe from the last query to a kafka broker at localhost:9092, with a topic called gear-position-updates
qry_gear_position.selectExpr("cast(truckId as string) as key", "cast(gearPosition as string) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "gear-position-updates") \
    .option("checkpointLocation","/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()


