from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    spark = (
        SparkSession.builder
        .appName("DWSLocationZone")
        .master("local[1]")
        .getOrCreate()
    )

    # ============================================================
    # 1. 读取 DWS 区域指标
    # ============================================================

    location_df = spark.read.parquet(
        "../../data/warehouse/dws/dws_location_taxi"
    )

    print("=" * 80)
    print("DWS 区域指标")
    print("=" * 80)

    location_df.show(10, truncate=False)


    # ============================================================
    # 2. 读取 Taxi Zone 维表
    # ============================================================

    zone_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(
            "../../data/warehouse/dimension/"
            "NYC_Taxi_Zones_20260828.csv"
        )
    )

    # 重命名字段
    zone_df = zone_df.dropDuplicates(["Location ID"])
    zone_df = zone_df.select(
        col("Location ID").alias("LocationID"),
        col("Zone"),
        col("Borough")
    )

    print("=" * 80)
    print("Zone 维表")
    print("=" * 80)

    zone_df.show(10, truncate=False)


    # ============================================================
    # 3. JOIN
    # ============================================================

    result_df = location_df.join(
        zone_df,
        location_df.PULocationID == zone_df.LocationID,
        "left"
    )

    # ============================================================
    # 4. 查看结果
    # ============================================================

    print("=" * 80)
    print("区域指标 + Zone 维表")
    print("=" * 80)

    result_df.select(
        "PULocationID",
        "Zone",
        "Borough",
        "trip_count",
        "total_revenue",
        "avg_revenue",
        "avg_distance"
    ).show(20, truncate=False)

    result_df.select(
    "PULocationID",
    "Zone",
    "Borough",
    "trip_count",
    "total_revenue",
    "avg_revenue",
    "avg_distance"
).write.mode("overwrite").parquet(
    "../../data/warehouse/dws/dws_location_zone"
)
    spark.stop()


if __name__ == "__main__":
    main()