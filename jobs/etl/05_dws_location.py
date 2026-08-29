from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round,
    row_number,
    rank,
    dense_rank
)
from pyspark.sql.window import Window


def main():

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_DWS_Location")
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
    # 2. 按上车区域统计
    # ============================================================

    location_df = (
        df
        .groupBy("PULocationID")
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
            ).alias("avg_distance")
        )
    )

    # ============================================================
    # 3. Window
    # ============================================================

    window_spec = Window.orderBy(
        location_df.trip_count.desc()
    )

    ranked_df = (
        location_df
        .withColumn(
            "row_number",
            row_number().over(window_spec)
        )
        .withColumn(
            "rank",
            rank().over(window_spec)
        )
        .withColumn(
            "dense_rank",
            dense_rank().over(window_spec)
        )
    )

    # ============================================================
    # 4. Top 20
    # ============================================================

    print("=" * 80)
    print("上车区域订单量 Top 20")
    print("=" * 80)

    ranked_df \
        .orderBy("row_number") \
        .show(20, truncate=False)

    # ============================================================
    # 5. 保存 DWS
    # ============================================================

    output_path = "../../data/warehouse/dws/dws_location_taxi"

    (
        ranked_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print("=" * 80)
    print("DWS 区域指标保存成功：")
    print(output_path)

    spark.stop()


if __name__ == "__main__":
    main()