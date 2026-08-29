import shutil

from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round,
    max,
    min,
    col
)

import etl_common as common


def main():

    # ============================================================
    # 1. 创建 SparkSession
    # ============================================================

    spark = common.create_spark("NYC_Taxi_DWS_Daily_Incremental")

    # ============================================================
    # 2. 找出需要重算的日期
    #
    # 需要重算的日期 = 两类：
    #   a. DWD 有、DWS 没有的日期（新增日期）
    #   b. DWD 分区比 DWS 分区更新（说明 DWD 这个日期被重算过，
    #      例如跨月边界 2-01 被 2 月数据补充过，DWS 必须跟着重算）
    #
    # 判断方法：对比 DWD / DWS 两边的分区目录，
    # 只是列目录 + 比文件修改时间，几乎不消耗计算资源。
    # ============================================================

    dwd_dates = common.list_partitions(
        common.DWD_PATH, "trip_date"
    )

    dws_dates = common.list_partitions(
        common.DWS_DAILY_PATH, "trip_date"
    )

    # ------------------------------------------------------------
    # 兼容旧版本：旧 dws_daily 是非分区写的（平铺文件），
    # 无法做分区增量，需要整体删掉重建一次。
    # ------------------------------------------------------------

    if not common.is_partitioned(
        common.DWS_DAILY_PATH, "trip_date"
    ):

        print("检测到旧的（非分区）dws_daily 数据，整体重建一次")
        shutil.rmtree(common.DWS_DAILY_PATH, ignore_errors=True)
        need_dates = set(dwd_dates)

    else:

        need_dates = set()

        for day in dwd_dates:

            if day not in dws_dates:
                # 新增日期
                need_dates.add(day)

            elif (
                common.mtime_of(common.DWD_PATH, "trip_date", day)
                > common.mtime_of(common.DWS_DAILY_PATH, "trip_date", day)
            ):
                # DWD 分区被更新过，DWS 需要跟着重算
                need_dates.add(day)

    print("需要重算的日期数：", len(need_dates))

    if not need_dates:
        print("=" * 80)
        print("DWS 每日指标已是最新，无需更新")
        print("=" * 80)
        spark.stop()
        return

    # ============================================================
    # 3. 只读取这些日期对应的 DWD 分区
    #
    # 关键点：trip_date 是分区字段，
    # filter(isin(...)) 会被 Spark 下推成"分区裁剪"，
    # 只扫描需要重算的分区文件，不去碰整张 DWD。
    # ============================================================

    df = (
        spark.read.parquet(common.DWD_PATH)
        .filter(
            col("trip_date")
            .isin(sorted(need_dates))
        )
    )

    # ============================================================
    # 4. 每日运营指标
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
    )

    # ============================================================
    # 5. 分区级覆盖保存 DWS（按 trip_date 分区）
    #
    # 先删除需要重算的旧分区，再 append 写入新分区。
    # ============================================================

    common.delete_partitions(
        common.DWS_DAILY_PATH, "trip_date", need_dates
    )

    (
        daily_df
        .write
        .mode("append")
        .partitionBy("trip_date")
        .parquet(common.DWS_DAILY_PATH)
    )

    print("=" * 80)
    print("DWS 每日指标增量更新完成")
    print("=" * 80)
    print("DWS 分区数：",
          len(common.list_partitions(common.DWS_DAILY_PATH, "trip_date")))

    # ============================================================
    # 6. 查看结果
    # ============================================================

    (
        spark.read.parquet(common.DWS_DAILY_PATH)
        .orderBy("trip_date")
        .show(45, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
