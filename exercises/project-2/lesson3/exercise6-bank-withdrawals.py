from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType, LongType
# Types avaiable: https://spark.apache.org/docs/2.1.1/api/python/_modules/pyspark/sql/types.html

# TODO: create bank withdrawals kafka message schema StructType including the following JSON elements:
#  {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
bankWithdrawalsMessageSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", FloatType()),
        StructField("dateAndTime", StringType()),
        StructField("transactionId", LongType()), # I think these might be too large for an IntegerType, so I'll try LongType
    ]   
)

# TODO: create an atm withdrawals kafka message schema StructType including the following JSON elements:
# {"transactionDate":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682,"atmLocation":"Thailand"}
atmWithdrawalsMessageSchema = StructType (
    [
        StructField("transactionDate", StringType()),
        StructField("transactionId", LongType()), 
        StructField("atmLocation", StringType()),
    ]
)

# TODO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("bank-withdrawals").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: read the bank-withdrawals kafka topic as a source into a streaming dataframe
# with the bootstrap server localhost:9092, 
# configuring the stream to read the earliest messages possible                                    
bank_withdrawals_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-withdrawals") \
    .option("startingOffsets","earliest") \
    .load() 

# TODO: using a select expression on the streaming dataframe, 
# cast the key and the value columns from kafka as strings, and then select them
bank_withdrawals_df = bank_withdrawals_stream.selectExpr("cast(key as string) key", "cast(value as string) value")

# TODO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TODO: create a temporary streaming view called "BankWithdrawals" 
# it can later be queried with spark.sql
bank_withdrawals_df.withColumn("value",from_json("value",bankWithdrawalsMessageSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("BankWithdrawals")

# TODO: using spark.sql, select * from BankWithdrawals into a dataframe
bank_withdrawals_qry = spark.sql("select * from BankWithdrawals")

# TODO: read the atm-withdrawals kafka topic as a source into a streaming dataframe
# with the bootstrap server localhost:9092, 
# configuring the stream to read the earliest messages possible                                    
atm_withdrawals_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","atm-withdrawals") \
    .option("startingOffsets","earliest") \
    .load() 

# TODO: using a select expression on the streaming dataframe, 
# cast the key and the value columns from kafka as strings, and then select them
atm_withdrawals_df = atm_withdrawals_stream.selectExpr("cast(key as string) key", "cast(value as string) value")

# TODO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TODO: create a temporary streaming view called "AtmWithdrawals" 
# it can later be queried with spark.sql
atm_withdrawals_df.withColumn("value",from_json("value",atmWithdrawalsMessageSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("AtmWithdrawals")

# TODO: using spark.sql, select * from AtmWithdrawals into a dataframe
atm_withdrawals_qry = spark.sql("select transactionDate, transactionId as atmTransactionId, atmLocation from AtmWithdrawals")

# TODO: join the atm withdrawals dataframe with the bank withdrawals dataframe
bank_atm_withdrawals_df = bank_withdrawals_qry.join(atm_withdrawals_qry, expr(
    """transactionId = atmTransactionId"""
))

# TODO: write the stream to the kafka in a topic called withdrawals-location, 
# and configure it to run indefinitely, the console will not output anything. 
# You will want to attach to the topic using the kafka-console-consumer inside another terminal, 
# it will look something like this:
# {
#   "accountNumber":"862939503",
#   "amount":"844.8",
#   "dateAndTime":"Oct 7, 2020 12:33:34 AM",
#   "transactionId":"1602030814320",
#   "transactionDate":"Oct 7, 2020 12:33:34 AM",
#   "atmTransactionId":"1602030814320",
#   "atmLocation":"Ukraine"
# }
bank_atm_withdrawals_df.selectExpr("cast(transactionId as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "withdrawals-location")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()

