from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    when,
    min,
    max,
    avg
)
def main():
    spark = (
        SparkSession.builder.appName("NYC_Taxi_Quality_Analysis").master("local[*]").getOrCreate()
    )
    input_path = "../data/raw/yellow_tripdata_2025-01.parquet"
    df = spark.read.parquet(input_path)

    total_count = df.count()

    print("="*80)
    print("1.数据总量")
    print("="*80)
    print(f"总记录数:{total_count}")


    print("="*80)
    print("2. passenger_count 分布")
    print("=" * 80)

    df.groupBy("passenger_count").count().orderBy("passenger_count").show(20)


    print("=" * 80)
    print("3. payment_type 分布")
    print("=" * 80)
    df.groupBy("payment_type").count().orderBy("payment_type").show()


    print("=" * 80)
    print("4. trip_distance 异常数据")
    print("=" * 80)

    distance_stats = df.select(count("*").alias("total"),
    sum(when(col("trip_distance") ==0,1).otherwise(0)).alias("distance_zero"),
    sum(when(col("trip_distance")<0,1).otherwise(0)).alias("distance_negative"),
    min("trip_distance").alias("min_distance"),
    max("trip_distance").alias("max_distance"),
    avg("trip_distance").alias("avg_distance")
    )
    distance_stats.show()
    print("=" * 80)
    print("5. fare_amount 异常数据")
    print("=" * 80)

    fare_stats = df.select(
        count("*").alias("total"),
        sum(when(col("fare_amount") < 0, 1).otherwise(0))
            .alias("fare_negative"),
        sum(when(col("fare_amount") == 0, 1).otherwise(0))
            .alias("fare_zero"),
        min("fare_amount").alias("fare_min"),
        max("fare_amount").alias("fare_max"),
        avg("fare_amount").alias("fare_avg")
    )

    fare_stats.show()

    # ============================================================
    # 6. total_amount 异常情况
    # ============================================================

    print("=" * 80)
    print("6. total_amount 异常数据")
    print("=" * 80)

    total_stats = df.select(
        count("*").alias("total"),
        sum(when(col("total_amount") < 0, 1).otherwise(0))
            .alias("total_negative"),
        sum(when(col("total_amount") == 0, 1).otherwise(0))
            .alias("total_zero"),
        min("total_amount").alias("total_min"),
        max("total_amount").alias("total_max"),
        avg("total_amount").alias("total_avg")
    )

    total_stats.show()

    # ============================================================
    # 7. 查看负金额数据
    # ============================================================

    print("=" * 80)
    print("7. 负金额数据示例")
    print("=" * 80)

    df.filter(
        col("total_amount") < 0
    ).select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "payment_type"
    ).show(20, truncate=False)

    # ============================================================
    # 8. 查看 payment_type = 0 的数据
    # ============================================================

    print("=" * 80)
    print("8. payment_type = 0 的数据")
    print("=" * 80)

    df.filter(
        col("payment_type") == 0
    ).select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount"
    ).show(20, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()  
