from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    unix_timestamp
)


def main():

    # 1. 创建 SparkSession
    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_Clean")
        .master("local[*]")
        .getOrCreate()
    )

    # ============================================================
    # 2. 读取原始数据
    # ============================================================

    input_path = "../data/raw/yellow_tripdata_2025-01.parquet"

    df = spark.read.parquet(input_path)

    print("=" * 80)
    print("原始数据量：", df.count())

    # ============================================================
    # 3. 查看空值情况
    # ============================================================

    print("=" * 80)
    print("字段空值统计：")

    for field in df.columns:
        count = df.filter(col(field).isNull()).count()
        print(f"{field}: {count}")

    # ============================================================
    # 4. 数据清洗
    # ============================================================

    clean_df = (
        df
        # 乘客数量不能小于等于 0
        .filter(col("passenger_count") > 0)

        # 行程距离必须大于 0
        .filter(col("trip_distance") > 0)

        # 下车时间必须晚于上车时间
        .filter(
            col("tpep_dropoff_datetime")
            > col("tpep_pickup_datetime")
        )

        # 总金额不能小于 0
        .filter(col("total_amount") >= 0)

        # 去掉关键字段为空的数据
        .filter(col("tpep_pickup_datetime").isNotNull())
        .filter(col("tpep_dropoff_datetime").isNotNull())
    )

    print("=" * 80)
    print("清洗后数据量：", clean_df.count())

    # ============================================================
    # 5. 增加业务字段
    # ============================================================

    clean_df = (
        clean_df

        # 出租车日期
        .withColumn(
            "trip_date",
            to_date(col("tpep_pickup_datetime"))
        )

        # 上车小时
        .withColumn(
            "pickup_hour",
            hour(col("tpep_pickup_datetime"))
        )

        # 行程时间，单位：分钟
        .withColumn(
            "trip_duration_minutes",
            (
                unix_timestamp(col("tpep_dropoff_datetime"))
                - unix_timestamp(col("tpep_pickup_datetime"))
            ) / 60
        )
    )

    # ============================================================
    # 6. 再次过滤异常行程时间
    # ============================================================

    clean_df = clean_df.filter(
        (col("trip_duration_minutes") > 0)
        & (col("trip_duration_minutes") <= 24 * 60)
    )

    print("=" * 80)
    print("最终数据量：", clean_df.count())

    # ============================================================
    # 7. 查看清洗后的数据
    # ============================================================

    print("=" * 80)
    print("清洗后的数据：")

    clean_df.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "total_amount",
        "trip_date",
        "pickup_hour",
        "trip_duration_minutes"
    ).show(10, truncate=False)

    # ============================================================
    # 8. 保存清洗结果
    # ============================================================

    output_path = "../data/processed/yellow_taxi_clean"

    clean_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("=" * 80)
    print("清洗后的数据已经保存到：")
    print(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
