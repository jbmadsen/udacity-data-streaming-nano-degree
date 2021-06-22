from pyspark.sql import SparkSession

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("exercise4-atm-visits").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
df_atm_visits_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","atm-visits") \
    .option("startingOffsets","earliest") \
    .load()

# TODO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
df_atm_visits = df_atm_visits_stream.selectExpr("cast(key as string) keyID", "cast(value as string) value")

# TODO: create a temporary streaming view called "ATMVisits" based on the streaming dataframe
df_atm_visits.createOrReplaceTempView("ATMVisits")

# TODO query the temporary view with spark.sql, with this query: "select * from ATMVisits"
qry_atm_visits = spark.sql("select * from ATMVisits")

# TODO: write the dataFrame from the last select statement to kafka to the atm-visit-updates topic, on the broker localhost:9092, and configure it to retrieve the earliest messages 
qry_atm_visits.selectExpr("cast(keyID as string) as key", "cast(value as string) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "atm-visit-updates") \
    .option("checkpointLocation","/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()
