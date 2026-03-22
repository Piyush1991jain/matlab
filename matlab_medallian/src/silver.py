from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

def main():
    df = spark.table("bronze.sensor_raw")

    clean_df = df.dropna() \
        .withColumn("value", col("value").cast("double"))

    clean_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver.sensor_clean")

if __name__ == "__main__":
    main()