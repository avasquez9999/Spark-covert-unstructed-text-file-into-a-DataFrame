# Spark-covert-unstructed-text-file-into-a-DataFrame
Spark covert unstructed text file into a DataFrame
This Python script (spark-sql.py) demonstrates fundamental functionalities of Apache Spark, particularly focusing on Spark SQL for structured data processing and analysis. It processes a dataset, fakefriends.csv, to extract insights using both SQL queries and the DataFrame API.

Code Breakdown:
Initialization of SparkSession:

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

This section initializes the SparkSession, which is the entry point for programming Spark with the DataFrame and SQL APIs. It sets up the application name as "SparkSQL." The Row class is imported to help define schema for the data.

Data Loading and Mapping (fakefriends.csv):

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

The fakefriends.csv file is loaded as an RDD (Resilient Distributed Dataset) of text lines using spark.sparkContext.textFile().

The mapper function processes each line. It splits the line by commas and converts the fields into a Row object, explicitly defining the data types (e.g., int for ID, str for name, int for age and numFriends). This step is crucial for structuring the data before it can be used with Spark SQL's DataFrames.

Schema Inference and DataFrame Creation:

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

spark.createDataFrame(people): This converts the RDD of Row objects into a Spark DataFrame named schemaPeople. Spark SQL is able to infer the schema (column names and their data types) directly from the Row objects.

.cache(): This caches the DataFrame in memory for faster access in subsequent operations.

schemaPeople.createOrReplaceTempView("people"): This registers the DataFrame as a temporary SQL view named "people". This allows you to query the DataFrame using standard SQL syntax, just as you would with a table in a relational database.

SQL Query Execution:

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19"): This demonstrates Spark SQL's primary feature: executing SQL queries on the registered DataFrame. It selects all columns from the "people" view where the age is between 13 and 19 (inclusive).

.collect(): This action brings all the results from the distributed DataFrame back to the driver program for printing. In a real-world big data scenario, you would typically perform further Spark transformations or save the results to a distributed storage system rather than collecting all data to the driver.

DataFrame API for Data Manipulation:

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

This section showcases the DataFrame API, which provides a more programmatic way to perform data transformations.

.groupBy("age"): Groups the DataFrame by the "age" column.

.count(): Counts the number of records within each age group.

.orderBy("age"): Sorts the results by age.

.show(): Displays the results in a formatted table. This achieves the same outcome as a SELECT age, COUNT(*) FROM people GROUP BY age ORDER BY age SQL query.

Stopping SparkSession:

spark.stop()

This gracefully shuts down the SparkSession and releases all associated resources.

Purpose and Learning Takeaways:
This script effectively illustrates:

How to initialize a Spark application.

Loading and structuring raw data into a Spark DataFrame.

The flexibility of Spark SQL, allowing data manipulation via both standard SQL queries and the programmatic DataFrame API.

Performing common data aggregation and filtering operations in a distributed environment.

It serves as a foundational example for anyone beginning to explore big data processing with Apache Spark.
