from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg
)


def main():

    spark = (
        SparkSession.builder
        .appName("ADSLocationDataQuality")
        .master("local[1]")
        .getOrCreate()
    )

    # ============================================================
    # 1. 读取 DWS 区域数据
    # ============================================================

    df = spark.read.parquet(
        "../../data/warehouse/dws/dws_location_zone"
    )

    print("=" * 80)
    print("读取 DWS 区域数据")
    print("=" * 80)

    df.show(10, truncate=False)

    # ============================================================
    # 2. 筛选 Zone / Borough 未匹配的数据
    # ============================================================

    invalid_df = df.filter(
        col("Zone").isNull() |
        col("Borough").isNull()
    )

    print("=" * 80)
    print("异常区域数据")
    print("=" * 80)

    invalid_df.show(20, truncate=False)

    # ============================================================
    # 3. 按 PULocationID 汇总异常数据
    # ============================================================

    quality_df = (
        invalid_df
        .groupBy("PULocationID")
        .agg(
            count("*").alias("record_count"),
            sum("trip_count").alias("trip_count"),
            sum("total_revenue").alias("total_revenue"),
            avg("avg_revenue").alias("avg_revenue")
        )
        .orderBy(
            col("trip_count").desc()
        )
    )

    print("=" * 80)
    print("ADS：Zone 维表未匹配数据")
    print("=" * 80)

    quality_df.show(
        20,
        truncate=False
    )

    # ============================================================
    # 4. 保存 ADS
    # ============================================================

    output_path = (
        "../../data/warehouse/ads/"
        "ads_location_data_quality"
    )

    quality_df.write \
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
        20,
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()