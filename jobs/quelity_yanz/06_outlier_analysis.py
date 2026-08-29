from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_Outlier_Analysis")
        .master("local[*]")
        .getOrCreate()
    )

    df = spark.read.parquet(
        "../data/raw/yellow_tripdata_2025-01.parquet"
    )

    # ============================================================
    # trip_distance 分段
    # ============================================================

    print("=" * 80)
    print("trip_distance 分段统计")
    print("=" * 80)

    distance_ranges = [
        ("0", 0, 0),
        ("0~5", 0, 5),
        ("5~10", 5, 10),
        ("10~20", 10, 20),
        ("20~50", 20, 50),
        ("50~100", 50, 100),
        ("100+", 100, float("inf"))
    ]

    for name, lower, upper in distance_ranges:

        if upper == float("inf"):
            count = df.filter(
                col("trip_distance") >= lower
            ).count()
        elif lower == upper:
            count = df.filter(
                col("trip_distance") == lower
            ).count()
        else:
            count = df.filter(
                (col("trip_distance") > lower)
                & (col("trip_distance") <= upper)
            ).count()

        print(f"{name:10s}: {count}")

    # ============================================================
    # fare_amount 分段
    # ============================================================

    print("=" * 80)
    print("fare_amount 分段统计")
    print("=" * 80)

    fare_ranges = [
        ("负数", None, 0),
        ("0~10", 0, 10),
        ("10~20", 10, 20),
        ("20~50", 20, 50),
        ("50~100", 50, 100),
        ("100+", 100, float("inf"))
    ]

    for name, lower, upper in fare_ranges:

        if lower is None:
            count = df.filter(
                col("fare_amount") < upper
            ).count()
        elif upper == float("inf"):
            count = df.filter(
                col("fare_amount") >= lower
            ).count()
        else:
            count = df.filter(
                (col("fare_amount") > lower)
                & (col("fare_amount") <= upper)
            ).count()

        print(f"{name:10s}: {count}")

    # ============================================================
    # total_amount 分段
    # ============================================================

    print("=" * 80)
    print("total_amount 分段统计")
    print("=" * 80)

    total_ranges = [
        ("负数", None, 0),
        ("0~20", 0, 20),
        ("20~50", 20, 50),
        ("50~100", 50, 100),
        ("100~500", 100, 500),
        ("500+", 500, float("inf"))
    ]

    for name, lower, upper in total_ranges:

        if lower is None:
            count = df.filter(
                col("total_amount") < upper
            ).count()
        elif upper == float("inf"):
            count = df.filter(
                col("total_amount") >= lower
            ).count()
        else:
            count = df.filter(
                (col("total_amount") > lower)
                & (col("total_amount") <= upper)
            ).count()

        print(f"{name:10s}: {count}")

    spark.stop()


if __name__ == "__main__":
    main()