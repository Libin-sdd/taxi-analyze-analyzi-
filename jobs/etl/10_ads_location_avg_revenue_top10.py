from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    spark = (
        SparkSession.builder
        .appName("ADSLocationAvgRevenueTop10")
        .master("local[1]")
        .getOrCreate()
    )

    # ============================================================
    # 1. 读取 DWS 区域 + Zone 数据
    # ============================================================

    df = spark.read.parquet(
        "../../data/warehouse/dws/dws_location_zone"
    )

    print("=" * 80)
    print("读取 DWS 区域数据")
    print("=" * 80)

    df.show(10, truncate=False)

    # ============================================================
    # 2. 平均客单价 Top 10
    # ============================================================

    avg_revenue_top10_df = (
        df
        .orderBy(
            col("avg_revenue").desc()
        )
        .select(
            "PULocationID",
            "Zone",
            "Borough",
            "trip_count",
            "total_revenue",
            "avg_revenue",
            "avg_distance"
        )
        .limit(10)
    )

    print("=" * 80)
    print("ADS：区域平均客单价 Top 10")
    print("=" * 80)

    avg_revenue_top10_df.show(
        10,
        truncate=False
    )

    # ============================================================
    # 3. 保存 ADS
    # ============================================================

    output_path = (
        "../../data/warehouse/ads/"
        "ads_location_avg_revenue_top10"
    )

    avg_revenue_top10_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("=" * 80)
    print("ADS 保存成功")
    print("=" * 80)

    print("保存路径：", output_path)

    # ============================================================
    # 4. 验证 ADS
    # ============================================================

    check_df = spark.read.parquet(
        output_path
    )

    print("=" * 80)
    print("读取 ADS 验证")
    print("=" * 80)

    check_df.show(
        10,
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()