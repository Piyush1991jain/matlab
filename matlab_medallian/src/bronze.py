from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def main():
    df = spark.read.csv("/Volumes/test/default/data/sample.csv", header=True, inferSchema=True)

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze.sensor_raw")

if __name__ == "__main__":
    main()