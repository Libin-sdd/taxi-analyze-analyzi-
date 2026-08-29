from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    # ============================================================
    # 1. 创建 SparkSession
    # ============================================================

    spark = (
        SparkSession.builder
        .appName("ADSLocationRevenueTop10")
        .master("local[1]")
        .getOrCreate()
    )

    # ============================================================
    # 2. 读取 DWS 区域 + Zone 数据
    # ============================================================

    df = spark.read.parquet(
        "../../data/warehouse/dws/dws_location_zone"
    )

    print("=" * 80)
    print("读取 DWS 区域数据")
    print("=" * 80)

    df.show(10, truncate=False)

    # ============================================================
    # 3. 区域收入 Top 10
    # ============================================================

    revenue_top10_df = (
        df
        .orderBy(
            col("total_revenue").desc()
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
    print("ADS：区域收入 Top 10")
    print("=" * 80)

    revenue_top10_df.show(
        10,
        truncate=False
    )

    # ============================================================
    # 4. 保存 ADS
    # ============================================================

    output_path = (
        "../../data/warehouse/ads/ads_location_revenue_top10"   
    )

    revenue_top10_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("=" * 80)
    print("ADS 保存成功")
    print("=" * 80)

    print("保存路径：", output_path)

    # ============================================================
    # 5. 验证 ADS
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