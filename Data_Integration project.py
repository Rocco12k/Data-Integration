# Databricks notebook source

# Load the CSV file into a Spark DataFrame
fixtures = spark.read.option("header", True).csv("/Volumes/workspace/my_schema/my_volume/fixtures.csv")

# Show the first 5 rows of the DataFrame
fixtures.show(5)


# COMMAND ----------

# 1. Import necessary PySpark functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# 2. (Optional) Create a Spark session (already available by default in Databricks)
spark = SparkSession.builder.appName("FixturesAnalysis").getOrCreate()

# COMMAND ----------

# 3. Load the fixtures data from CSV file located in Unity Catalog Volume
fixtures = spark.read.option("header", True).csv("/Volumes/workspace/my_schema/my_volume/fixtures.csv")

# COMMAND ----------

# Show the first 5 rows to check the data
fixtures.show(5)


# COMMAND ----------

# 4. Check the data schema to understand column names and data types
fixtures.printSchema()

# COMMAND ----------

# 5. Data Cleaning
# Drop any rows with missing values (nulls)
fixtures_clean = fixtures.dropna()

# Remove duplicate rows, if any exist
fixtures_clean = fixtures_clean.dropDuplicates()

# Show cleaned data
fixtures_clean.show(5)

# COMMAND ----------

# 6. Basic Analytics & Insights

# A. Number of matches per team (group by 'team' column and count)
matches_per_team = fixtures_clean.groupBy("Home").count().orderBy(col("count").desc())
matches_per_team.show(5)

# B. Most common kick-off times (group by 'kick_off' column and count)
kick_off_counts = fixtures_clean.groupBy("Time").count().orderBy(col("count").desc())
kick_off_counts.show(5)

# C. Number of matches on each date (group by 'date' column and count)
matches_per_date = fixtures_clean.groupBy("Date").count().orderBy(col("date"))
matches_per_date.show(10)

# COMMAND ----------

# 7. Save cleaned data as a Parquet file in your volume for efficient storage and querying
fixtures_clean.write.mode("overwrite").parquet("/Volumes/workspace/my_schema/my_volume/cleaned_fixtures.parquet")

# COMMAND ----------

# 8. Visualizations using Databricks' built-in display() function

# A. Number of matches per team
display(matches_per_team)

# B. Number of matches per date
display(matches_per_date)