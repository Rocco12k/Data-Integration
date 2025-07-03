from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#I load my dataset from my unity catalog volume into the Spark DataFrame
fixtures = spark.read.option("header", True).csv("/Volumes/workspace/my_schema/my_volume/fixtures.csv")

#Seeing the first 5 rows to check the data
fixtures.show(5)

#Printing the schema of the dataset to check the column names and their data types.
fixtures.printSchema()

#Removing missing values for eg. (nulls), duplicate rows, and show the cleane data
fixtures_clean = fixtures.dropna()
fixtures_clean = fixtures_clean.dropDuplicates()
fixtures_clean.show(5)


#Making queries 
#1.Number of matches per team 
matches_per_team = fixtures_clean.groupBy("Home").count().orderBy(col("count").desc())
matches_per_team.show(5)

#2.Most common kick-off times
kick_off_counts = fixtures_clean.groupBy("Time").count().orderBy(col("count").desc())
kick_off_counts.show(5)

#3.Number of matches on each date
matches_per_date = fixtures_clean.groupBy("Date").count().orderBy(col("date"))
matches_per_date.show(10)

#Saving the now cleaned data as a Parquet file in your volume for efficient storage and querying
fixtures_clean.write.mode("overwrite").parquet("/Volumes/workspace/my_schema/my_volume/cleaned_fixtures.parquet")

# Basic visualizations
#1.Number of matches per team
display(matches_per_team)

#2.Number of matches per date
display(matches_per_date)
