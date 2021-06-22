from pyspark.sql import SparkSession

# TODO: create a variable with the absolute path to the text file
file_log = "/home/workspace/Test.txt"

# TODO: create a Spark session
spark = SparkSession.builder.appName("exercise-1-hellospark").getOrCreate()

# TODO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TODO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 
df_log = spark.read.text(file_log).cache()

# TODO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found
a = df_log.filter(df_log.value.contains('a')).count()

# TODO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
b = df_log.filter(df_log.value.contains('b')).count()

# TODO: print the count for letter 'd' and letter 's'
print(f"Lines with a: {a}, lines with b: {b}")

# TODO: stop the spark application
spark.stop()
