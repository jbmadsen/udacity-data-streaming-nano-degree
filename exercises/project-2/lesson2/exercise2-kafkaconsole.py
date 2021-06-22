from pyspark.sql import SparkSession

# TODO: create a Spak Session, and name the app something relevant
spark = SparkSession.builder.appName("exercise2-kafkaconsole").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: read a stream from the kafka topic 'fuel-level', with the bootstrap server localhost:9092, reading from the earliest message
df_fuel_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","fuel-level") \
    .option("startingOffsets","earliest") \
    .load()

# TODO: cast the key and value columns as strings and select them using a select expression function
df_fuel = df_fuel_stream.selectExpr("cast(key as string) key", "cast(value as string) value")

# TODO: write the dataframe to the console, and keep running indefinitely
df_fuel.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

