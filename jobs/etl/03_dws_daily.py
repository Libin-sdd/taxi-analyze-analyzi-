from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round,
    max,
    min
)


def main():

    # ============================================================
    # 1. 创建 SparkSession
    # ============================================================

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_DWS_Daily")
        .master("local[*]")
        .getOrCreate()
    )

    # ============================================================
    # 2. 读取 DWD
    # ============================================================

    input_path = "../../data/warehouse/dwd/dwd_taxi_trip"

    df = spark.read.parquet(input_path)

    print("=" * 80)
    print("DWD 数据量：", df.count())

    # ============================================================
    # 3. 每日运营指标
    # ============================================================

    daily_df = (
        df
        .groupBy("trip_date")
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
            ).alias("avg_duration"),

            round(
                min("trip_duration_minutes"),
                2
            ).alias("min_duration"),

            round(
                max("trip_duration_minutes"),
                2
            ).alias("max_duration")
        )
        .orderBy("trip_date")
    )

    # ============================================================
    # 4. 查看结果
    # ============================================================

    print("=" * 80)
    print("每日运营指标")
    print("=" * 80)

    daily_df.show(31, truncate=False)

    # ============================================================
    # 5. 保存 DWS
    # ============================================================

    output_path = "../../data/warehouse/dws/dws_daily_taxi"

    (
        daily_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print("=" * 80)
    print("DWS 保存成功：")
    print(output_path)

    spark.stop()


if __name__ == "__main__":
    main()