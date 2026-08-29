from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (SparkSession.builder.appName("NYC_Taxi_Data_Quality").master("local[*]").getOrCreate())
    input_path = "../data/raw/yellow_tripdata_2025-01.parquet"
    df = spark.read.parquet(input_path)
    print("=" * 80)
    print("原始数据量：", df.count())
    null_passenger = df.filter(col("passenger_count").isNull())

    print("="*80)
    print("passenger_count 为空的数据量：")
    print(null_passenger.count())

    print("="*80)
    print("这些数据的基本情况")

    null_passenger.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "total_amount"    
    ).show(20,truncate=False)

    spark.stop()
if __name__ == "__main__":
    main()
