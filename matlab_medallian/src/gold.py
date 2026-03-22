from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.getOrCreate()

def main():
    df = spark.table("silver.sensor_clean")

    gold_df = df.withColumn(
        "status",
        when(col("value") > 90, "CRITICAL")
        .when(col("value") > 70, "WARNING")
        .otherwise("NORMAL")
    )

    gold_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("gold.sensor_alerts")

if __name__ == "__main__":
    main()