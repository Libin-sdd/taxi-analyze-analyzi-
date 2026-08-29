from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round
)


def main():

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_DWS_Hourly")
        .master("local[*]")
        .getOrCreate()
    )

    # ============================================================
    # 1. 读取 DWD
    # ============================================================

    input_path = "../../data/warehouse/dwd/dwd_taxi_trip"

    df = spark.read.parquet(input_path)

    print("=" * 80)
    print("DWD 数据量：", df.count())

    # ============================================================
    # 2. 按小时统计
    # ============================================================

    hourly_df = (
        df
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("trip_count"),

            round(
                sum("total_amount"),
                2
            ).alias("total_revenue"),

            round(
                avg("total_amount"),
                2
            ).alias("avg_revenue"),

            round(
                avg("trip_distance"),
                2
            ).alias("avg_distance"),

            round(
                avg("trip_duration_minutes"),
                2
            ).alias("avg_duration")
        )
        .orderBy("pickup_hour")
    )

    # ============================================================
    # 3. 查看结果
    # ============================================================

    print("=" * 80)
    print("每小时运营指标")
    print("=" * 80)

    hourly_df.show(24, truncate=False)

    # ============================================================
    # 4. 找出订单量最高的 5 个小时
    # ============================================================

    print("=" * 80)
    print("订单量 Top 5 小时")
    print("=" * 80)

    hourly_df \
        .orderBy(hourly_df.trip_count.desc()) \
        .show(5)

    # ============================================================
    # 5. 保存 DWS
    # ============================================================

    output_path = "../../data/warehouse/dws/dws_hourly_taxi"

    (
        hourly_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print("=" * 80)
    print("DWS 小时指标保存成功：")
    print(output_path)

    spark.stop()


if __name__ == "__main__":
    main()