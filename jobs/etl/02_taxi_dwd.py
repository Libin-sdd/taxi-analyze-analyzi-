from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    unix_timestamp,
    round
)


def main():

    # ============================================================
    # 1. 创建 SparkSession
    # ============================================================

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_DWD")
        .master("local[*]")
        .getOrCreate()
    )

    # ============================================================
    # 2. 读取 ODS 原始数据
    # ============================================================

    input_path = "../../data/raw/yellow_tripdata_2025-01.parquet"

    df = spark.read.parquet(input_path)

    original_count = df.count()

    print("=" * 80)
    print("DWD ETL 开始")
    print("=" * 80)
    print("ODS 原始数据量：", original_count)

    # ============================================================
    # 3. 数据清洗
    # ============================================================

    dwd_df = (
        df

        # 核心时间字段不能 NULL
        .filter(col("tpep_pickup_datetime").isNotNull())
        .filter(col("tpep_dropoff_datetime").isNotNull())

        # 下车时间必须晚于上车时间
        .filter(
            col("tpep_dropoff_datetime")
            > col("tpep_pickup_datetime")
        )

        # 行程距离
        .filter(col("trip_distance") > 0)
        .filter(col("trip_distance") <= 100)

        # 上下车区域不能为空
        .filter(col("PULocationID").isNotNull())
        .filter(col("DOLocationID").isNotNull())
    )

    after_clean_count = dwd_df.count()

    print("=" * 80)
    print("基础清洗完成")
    print("清洗后数据量：", after_clean_count)
    print("过滤数据量：", original_count - after_clean_count)

    # ============================================================
    # 4. 增加维度字段
    # ============================================================

    dwd_df = (
        dwd_df

        # 行程日期
        .withColumn(
            "trip_date",
            to_date(col("tpep_pickup_datetime"))
        )

        # 上车小时
        .withColumn(
            "pickup_hour",
            hour(col("tpep_pickup_datetime"))
        )

        # 行程时间（分钟）
        .withColumn(
            "trip_duration_minutes",
            round(
                (
                    unix_timestamp(
                        col("tpep_dropoff_datetime")
                    )
                    -
                    unix_timestamp(
                        col("tpep_pickup_datetime")
                    )
                ) / 60,
                2
            )
        )
    )

    # ============================================================
    # 5. 行程时间再次检查
    # ============================================================

    dwd_df = dwd_df.filter(
        (col("trip_duration_minutes") > 0)
        & (col("trip_duration_minutes") <= 1440)
    )

    final_count = dwd_df.count()

    print("=" * 80)
    print("DWD 最终数据量：", final_count)
    print("最终过滤数据量：", original_count - final_count)

    # ============================================================
    # 6. 查看 DWD 数据
    # ============================================================

    print("=" * 80)
    print("DWD 数据示例")
    print("=" * 80)

    dwd_df.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_date",
        "pickup_hour",
        "trip_duration_minutes",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount"
    ).show(10, truncate=False)

    # ============================================================
    # 7. 保存 DWD
    # ============================================================

    output_path = "../../data/warehouse/dwd/dwd_taxi_trip"

    (
        dwd_df
        .write
        .mode("overwrite")
        .partitionBy("trip_date")
        .parquet(output_path)
    )

    print("=" * 80)
    print("DWD 数据保存成功：")
    print(output_path)

    spark.stop()


if __name__ == "__main__":
    main()