from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_Distribution")
        .master("local[*]")
        .getOrCreate()
    )

    df = spark.read.parquet(
        "../data/raw/yellow_tripdata_2025-01.parquet"
    )

    print("=" * 80)
    print("trip_distance 分布")
    print("=" * 80)

    df.select("trip_distance").summary(
        "count",
        "min",
        "25%",
        "50%",
        "75%",
        "max"
    ).show()

    print("=" * 80)
    print("trip_distance > 100")
    print("=" * 80)

    df.filter(
        col("trip_distance") > 100
    ).select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "PULocationID",
        "DOLocationID"
    ).orderBy(
        col("trip_distance").desc()
    ).show(20, truncate=False)

    print("=" * 80)
    print("fare_amount 分布")
    print("=" * 80)

    df.select("fare_amount").summary(
        "count",
        "min",
        "25%",
        "50%",
        "75%",
        "max"
    ).show()

    print("=" * 80)
    print("total_amount 分布")
    print("=" * 80)

    df.select("total_amount").summary(
        "count",
        "min",
        "25%",
        "50%",
        "75%",
        "max"
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()