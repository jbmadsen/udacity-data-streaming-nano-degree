from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr, lit
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

# TODO: create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
#
# Schema, example (from README):
# {
#     "key":"__Q3VzdG9tZXI=__",
#     "existType":"NONE",
#     "Ch":false,
#     "Incr":false,
#     "zSetEntries":[{
#         "element":"__eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NT# EyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==__",
#         "Score":0.0
#     }],
#     "zsetEntries":[{
#         "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEy# MTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
#         "score":0.0
#     }]
# }
redis_server_topic_schema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()), \
                StructField("score", DoubleType()) \
            ]) \
        )), \
    ]
)

# TODO: create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
#
# Schema, example (from sparkpykafkajoin.py):
# {
#   "customerName":"Sam Test",
#   "email":"sam.test@test.com",
#   "phone":"8015551212",
#   "birthDay":"2001-01-03"
# }
customer_schema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

# TODO: create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
#
# Schema, example (from README):
# {
#     "customer":"Jason.Mitra@test.com",
#     "score":7.0,
#     "riskDate":"2020-09-14T07:54:06.417Z"
# }
customer_risk_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", DoubleType()),
        StructField("riskDate", DateType())
    ]
)

# TODO: create a spark application object
spark = SparkSession.builder.appName("customer-data").getOrCreate()

# TODO: set the spark log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redis_server_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# TODO: cast the value column in the streaming dataframe as a STRING 
redis_server_stream = redis_server_stream.selectExpr("cast(value as string) value")

# TODO:; parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {
#   "key":"Q3VzdG9tZXI=",
#   "existType":"NONE",
#   "Ch":false,
#   "Incr":false,
#   "zSetEntries":[{
#       "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
#       "Score":0.0
#   }],
#   "zsetEntries":[{
#       "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
#       "score":0.0
#   }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
column_names = ('value.key', 'value.existType', 'value.Ch', 'value.Incr', 'value.zSetEntries')  # Columns we need
# In order to format per requirements, we:
# 1) select the column names,
# 2) rename to lowercase where needed, 
# 3) all columns with default null values (lit(None)): https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.lit.html
# 4) Create data into view
redis_server_stream.withColumn("value", from_json("value", redis_server_topic_schema)) \
    .select(*column_names) \
    .withColumnRenamed('Ch', 'ch') \
    .withColumnRenamed('Incr', 'incr') \
    .withColumn('value', lit(None).cast(StringType())) \
    .withColumn('expiredType', lit(None).cast(StringType())) \
    .withColumn('expiredValue', lit(None).cast(StringType())) \
    .createOrReplaceTempView("RedisSortedSet")

# TODO: execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
# Note: We did something similar in exercise7-current-country.py
encoded_customer_stream = spark.sql(
    """
    select 
        key, 
        zSetEntries[0].element as encodedCustomer 
    from RedisSortedSet
    """
)

# TODO: take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
# Note: We did something similar in exercise7-current-country.py
encoded_customer_stream_decoded = encoded_customer_stream \
    .withColumn("customer", unbase64(encoded_customer_stream.encodedCustomer).cast("string"))

# TODO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords
encoded_customer_stream_decoded \
    .withColumn("customer", from_json("customer", customer_schema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("CustomerRecords")

# TODO: JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql(
    """
    select 
        email,
        birthDay 
    from CustomerRecords 
    where email is not null 
        and birthDay is not null
    """
)

# TODO: Split the birth year as a separate field from the birthday
# TODO: Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
# Note: Split and alias as oneline example: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.split.html
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .select('email', split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear"))

# TODO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
stedi_events_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# TODO: cast the value column in the streaming dataframe as a STRING 
stedi_events_stream = stedi_events_stream.selectExpr("cast(value as string) value")

# TODO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
stedi_events_stream \
    .withColumn("value", from_json("value", customer_risk_schema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("CustomerRisk")

# TODO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("""
    select 
        customer, 
        score 
    from CustomerRisk
"""
)

# TODO: join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
# Note: Allmost copy/paste from exercise7-current-country.py
riskScoreByBirthYear = customerRiskStreamingDF.join(emailAndBirthYearStreamingDF, expr("""
    customer = email
"""
))

# TODO: sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format 
# {
#   "customer":"Santosh.Fibonnaci@test.com",
#   "score":"28.5",
#   "email":"Santosh.Fibonnaci@test.com",
#   "birthYear":"1963"
# }
# Note: Allmost copy/paste from exercise5-customer-record.py
riskScoreByBirthYear \
    .selectExpr("CAST(customer AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stedi-events.customer.risk-score") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()
