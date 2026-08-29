from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def main():

    # ============================================================
    # 1. 创建 SparkSession
    # ============================================================

    spark = (
        SparkSession.builder
        .appName("ADSBoroughLocationTop3")
        .master("local[1]")
        .getOrCreate()
    )

    # ============================================================
    # 2. 读取 DWS 区域 + Zone 数据
    # ============================================================

    df = spark.read.parquet(
        "../../data/warehouse/dws/dws_location_zone"
    )
    df = df.filter(
    col("Borough").isNotNull()
)
    print("=" * 80)
    print("读取 DWS 区域数据")
    print("=" * 80)

    df.show(10, truncate=False)

    # ============================================================
    # 3. 创建窗口
    #
    # 每一个 Borough 单独排名
    # 每个 Borough 内按照订单量降序
    # ============================================================

    window_spec = (
        Window
        .partitionBy("Borough")
        .orderBy(
            col("trip_count").desc()
        )
    )

    # ============================================================
    # 4. 计算每个 Borough 内的排名
    # ============================================================

    ranked_df = df.withColumn(
        "location_rank",
        row_number().over(window_spec)
    )

    # ============================================================
    # 5. 每个 Borough 只保留 Top 3
    # ============================================================

    top3_df = (
        ranked_df
        .filter(
            col("location_rank") <= 3
        )
        .select(
            "Borough",
            "location_rank",
            "PULocationID",
            "Zone",
            "trip_count",
            "total_revenue",
            "avg_revenue",
            "avg_distance"
        )
        .orderBy(
            "Borough",
            "location_rank"
        )
    )

    print("=" * 80)
    print("ADS：每个 Borough 订单量 Top 3")
    print("=" * 80)

    top3_df.show(
        100,
        truncate=False
    )

    # ============================================================
    # 6. 保存 ADS
    # ============================================================

    output_path = (
        "../../data/warehouse/ads/"
        "ads_borough_location_top3"
    )

    top3_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("=" * 80)
    print("ADS 保存成功")
    print("=" * 80)

    print("保存路径：", output_path)

    # ============================================================
    # 7. 验证 ADS
    # ============================================================

    check_df = spark.read.parquet(
        output_path
    )

    print("=" * 80)
    print("读取 ADS 验证")
    print("=" * 80)

    check_df.show(
        100,
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()