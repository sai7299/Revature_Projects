from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Filter Failed Transactions") \
    .getOrCreate()

# Input: Combined cleaned CSV file from previous job
input_path = "gs://gcp-p1/output/cleaned_transactions.csv"

# Output: Filtered failed transactions
output_path = "gs://gcp-p1/output/failed_txns.csv"

# Read cleaned data
df = spark.read.option("header", True).csv(input_path)

# Filter for failed transactions (case insensitive)
failed_df = df.filter(col("status").isin("FAILED", "failed", "Fail", "fail"))

# Drop rows with missing critical info
failed_df = failed_df.dropna(subset=["transaction_id", "amount", "date"])

# Write the result to GCS
failed_df.coalesce(1).write \
    .option("header", True) \
    .mode("overwrite") \
    .csv(output_path)

print("✅ Filtered failed transactions written to GCS.")
spark.stop()