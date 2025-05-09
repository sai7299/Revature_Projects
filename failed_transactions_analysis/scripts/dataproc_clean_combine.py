from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Clean and Combine Banking Transactions") \
    .getOrCreate()

# Input folder: all raw CSVs uploaded from bank branches
input_path = "gs://gcp-p1/data/"

# Output file: single cleaned and combined file
output_path = "gs://gcp-p1/output/cleaned_transactions.csv"

# Read all CSVs in the input path
df = spark.read.option("header", True).csv(input_path)

# Drop completely empty rows
df = df.dropna(how='all')

# Drop rows with nulls in essential fields
df = df.dropna(subset=["transaction_id", "amount", "status", "city", "branch", "date"])

# Optional: trim strings to remove leading/trailing spaces
columns_to_trim = ["transaction_id", "status", "city", "branch", "date"]
for col_name in columns_to_trim:
    df = df.withColumn(col_name, trim(col(col_name)))

# Write combined, cleaned data to output path
df.coalesce(1).write \
    .option("header", True) \
    .mode("overwrite") \
    .csv(output_path)

print("✅ Combined and cleaned CSV written to output.")
spark.stop()
