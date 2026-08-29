from pyspark.sql import SparkSession

def main():
    spark = (
            SparkSession.builder
            .appName("NYC_Taxi_Read")
            .master("local[*]")
            .getOrCreate()
            )
    input_path = "../data/raw/yellow_tripdata_2025-01.parquet"
    df = spark.read.parquet(input_path)
    print("=" * 80)
    print("数据总行数：")
    print(df.count())

    print("="*80)
    print("前十条数据:")
    df.show(10,truncate=False)
    spark.stop()
if __name__ == "__main__":
    main()
