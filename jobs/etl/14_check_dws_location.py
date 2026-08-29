from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def main():

    spark = (
        SparkSession.builder
        .appName("CheckDWSLocation")
        .master("local[1]")
        .getOrCreate()
    )

    # 读取 DWS
    dws_df = spark.read.parquet(
        "../../data/warehouse/dws/dws_location_zone"
    )

    print("=" * 80)
    print("DWS 数据结构")
    print("=" * 80)

    dws_df.printSchema()

    print("=" * 80)
    print("DWS 总订单量")
    print("=" * 80)

    dws_df.select(
        sum("trip_count").alias("total_trip_count")
    ).show()

    print("=" * 80)
    print("LocationID 重复检查")
    print("=" * 80)

    duplicate_df = (
        dws_df
        .groupBy("PULocationID")
        .count()
        .filter(col("count") > 1)
        .orderBy(col("count").desc())
    )

    duplicate_df.show(50, truncate=False)
    print("=" * 80)
    print("检查 PULocationID = 56")
    print("=" * 80)

    dws_df.filter(
        col("PULocationID") == 56
    ).show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()