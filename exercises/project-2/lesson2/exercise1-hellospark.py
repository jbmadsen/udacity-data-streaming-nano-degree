from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
file_log = "/home/workspace/Test.txt"

# TO-DO: create a Spark session
spark = SparkSession.builder.appName("exercise-1-hellospark").getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 
df_log = spark.read.text(file_log).cache()

# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found
a = df_log.filter(df_log.value.contains('a')).count()

# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
b = df_log.filter(df_log.value.contains('b')).count()

# TO-DO: print the count for letter 'd' and letter 's'
print(f"Lines with a: {a}, lines with b: {b}")

# TO-DO: stop the spark application
spark.stop()
