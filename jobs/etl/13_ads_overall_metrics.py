from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum,
    col,
    round as spark_round
)


def main():

    # ============================================================
    # 1. 创建 SparkSession
    # ============================================================
    spark = (
        SparkSession.builder
        .appName("ADS_Overall_Operation_Metrics")
        .master("local[1]")
        .getOrCreate()
    )

    # ============================================================
    # 2. 路径配置
    # ============================================================

    # DWS：已经计算好的区域指标
    dws_path = "../../data/warehouse/dws/dws_location_zone"

    # ADS：整体运营指标
    ads_path = "../../data/warehouse/ads/ads_overall_metrics"

    # ============================================================
    # 3. 读取 DWS 区域数据
    # ============================================================

    print("=" * 80)
    print("读取 DWS 区域数据")
    print("=" * 80)

    dws_df = spark.read.parquet(dws_path)

    dws_df.show(10, truncate=False)

    print("DWS 数据量：", dws_df.count())

    # ============================================================
    # 4. 查看 DWS 数据结构
    # ============================================================

    print("=" * 80)
    print("DWS 数据结构")
    print("=" * 80)

    dws_df.printSchema()

    # ============================================================
    # 5. 计算 ADS 整体运营指标
    #
    # 注意：
    #
    # 不能使用：
    #
    # avg("avg_revenue")
    #
    # 因为这只是：
    # 各区域平均客单价的简单平均
    #
    # 正确的整体客单价：
    #
    # 总收入 / 总订单数
    #
    # ============================================================

    print("=" * 80)
    print("计算 ADS：整体运营指标")
    print("=" * 80)

    overall_df = (
        dws_df
        .agg(
            # 总订单量
            sum("trip_count").alias("total_trip_count"),

            # 总收入
            sum("total_revenue").alias("total_revenue"),

            # 用订单量作为权重计算总里程
            (
                sum(
                    col("avg_distance") *
                    col("trip_count")
                )
            ).alias("weighted_distance")
        )

        # --------------------------------------------------------
        # 真正的整体平均客单价
        #
        # total_revenue / total_trip_count
        # --------------------------------------------------------
        .withColumn(
            "avg_revenue",
            spark_round(
                col("total_revenue") /
                col("total_trip_count"),
                2
            )
        )

        # --------------------------------------------------------
        # 真正的整体平均距离
        #
        # Σ(区域平均距离 × 区域订单量)
        # ---------------------------------------------
        #              总订单量
        # --------------------------------------------------------
        .withColumn(
            "avg_distance",
            spark_round(
                col("weighted_distance") /
                col("total_trip_count"),
                2
            )
        )

        # --------------------------------------------------------
        # 最终 ADS 字段
        # --------------------------------------------------------
        .select(
            "total_trip_count",
            "total_revenue",
            "avg_revenue",
            "avg_distance"
        )
    )

    # ============================================================
    # 6. 显示计算结果
    # ============================================================

    print("=" * 80)
    print("ADS：整体运营指标")
    print("=" * 80)

    overall_df.show(
        truncate=False
    )

    # ============================================================
    # 7. 保存 ADS
    #
    # 使用 overwrite：
    # 每次重新计算都覆盖旧 ADS
    # 不追加、不产生重复数据
    # ============================================================

    print("=" * 80)
    print("保存 ADS")
    print("=" * 80)

    overall_df.write \
        .mode("overwrite") \
        .parquet(ads_path)

    print("ADS 保存成功")
    print("保存路径：", ads_path)

    # ============================================================
    # 8. 重新读取 ADS 验证
    # ============================================================

    print("=" * 80)
    print("读取 ADS 验证")
    print("=" * 80)

    ads_df = spark.read.parquet(
        ads_path
    )

    ads_df.show(
        truncate=False
    )

    # ============================================================
    # 9. 数据质量验证
    # ============================================================

    print("=" * 80)
    print("ADS 数据质量验证")
    print("=" * 80)

    ads_df.printSchema()

    print("ADS 记录数：", ads_df.count())

    # ============================================================
    # 10. 与 DWS 订单量进行一致性检查
    # ============================================================

    dws_trip_count = (
        dws_df
        .agg(
            sum("trip_count").alias("total_trip_count")
        )
        .collect()[0]["total_trip_count"]
    )

    ads_trip_count = (
        ads_df
        .collect()[0]["total_trip_count"]
    )

    print("=" * 80)
    print("DWS / ADS 订单量一致性检查")
    print("=" * 80)

    print("DWS 总订单量：", dws_trip_count)
    print("ADS 总订单量：", ads_trip_count)

    if dws_trip_count == ads_trip_count:
        print("✓ DWS / ADS 订单量一致")
    else:
        print("✗ DWS / ADS 订单量不一致")

    # ============================================================
    # 11. 计算真正的整体客单价用于再次验证
    # ============================================================

    total_revenue = (
        ads_df
        .collect()[0]["total_revenue"]
    )

    total_trip_count = (
        ads_df
        .collect()[0]["total_trip_count"]
    )

    avg_revenue = (
        total_revenue /
        total_trip_count
    )

    print("=" * 80)
    print("整体客单价验证")
    print("=" * 80)

    print(
        "总收入：",
        total_revenue
    )

    print(
        "总订单量：",
        total_trip_count
    )

    print(
        "总收入 / 总订单量 =",
        round(avg_revenue, 2)
    )

    # ============================================================
    # 12. 关闭 Spark
    # ============================================================

    spark.stop()


if __name__ == "__main__":
    main()
