from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType

# TODO: create a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
kafkaMessageSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", IntegerType()),
        StructField("dateAndTime", StringType())
    ]
)

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("exercise2-bank-deposits").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
bank_deposits_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-deposits") \
    .option("startingOffsets","earliest") \
    .load()      

# TODO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
bank_deposits_df = bank_deposits_stream.selectExpr("cast(key as string) key", "cast(value as string) value")

# TODO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TODO: create a temporary streaming view called "BankDeposits"
# It can later be queried with spark.sql
bank_deposits_df.withColumn("value",from_json("value",kafkaMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("BankDeposits")


# TODO: using spark.sql, select * from BankDeposits
bank_deposits_qry = spark.sql("select * from BankDeposits")

# TODO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-------------+------+--------------------+
# |accountNumber|amount|         dateAndTime|
# +-------------+------+--------------------+
# |    103397629| 800.8|Oct 6, 2020 1:27:...|
# +-------------+------+--------------------+
bank_deposits_qry \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()



