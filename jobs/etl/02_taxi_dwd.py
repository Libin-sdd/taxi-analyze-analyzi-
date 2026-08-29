from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    unix_timestamp,
    round
)

import etl_common as common


# ============================================================
# 1. 清洗函数
#
# 把"原始数据 -> 清洗 -> 加工字段"做成一个函数，
# 增量时每个新月份都会调用一次。
# ============================================================

def clean_dwd(df):
    """
    输入：某个月的原始数据（DataFrame）
    输出：清洗 + 加工字段后的 DWD 数据

    清洗规则与原来完全一致：
        1. 核心时间字段不能 NULL
        2. 下车时间必须晚于上车时间
        3. trip_distance > 0 且 <= 100
        4. PULocationID / DOLocationID 不能为空
        5. 行程时长 > 0 且 <= 1440 分钟
    """
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

        # 行程日期 / 上车小时 / 行程时长（分钟）
        .withColumn(
            "trip_date",
            to_date(col("tpep_pickup_datetime"))
        )
        .withColumn(
            "pickup_hour",
            hour(col("tpep_pickup_datetime"))
        )
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

        # 行程时间再次检查
        .filter(
            (col("trip_duration_minutes") > 0)
            & (col("trip_duration_minutes") <= 1440)
        )
    )

    return dwd_df


def main():

    # ============================================================
    # 1. 创建 SparkSession
    # ============================================================

    spark = common.create_spark("NYC_Taxi_DWD_Incremental")

    # ============================================================
    # 2. 自动扫描 raw 目录，发现所有月份文件
    #
    # 不再写死 2025-01。
    # 以后 raw 目录里放新文件，这里会自动识别。
    # ============================================================

    raw_files = common.list_raw_files()

    print("=" * 80)
    print("发现原始文件")
    print("=" * 80)

    for year_month, path in raw_files:
        print(f"  {year_month}  {path}")

    # ------------------------------------------------------------
    # DWD 已有的 trip_date 分区
    # ------------------------------------------------------------

    existing_dates = common.list_partitions(
        common.DWD_PATH, "trip_date"
    )

    print("DWD 已有分区数：", len(existing_dates))

    # ------------------------------------------------------------
    # 找出还需要处理的新月份
    # 判断标准：该月每一天的 trip_date 都已经在 DWD 里
    # ------------------------------------------------------------

    new_months = [
        (year_month, path)
        for year_month, path in raw_files
        if not common.month_is_covered(
            year_month, existing_dates
        )
    ]

    print("需要处理的月份：",
          [ym for ym, _ in new_months] or "无（DWD 已是最新）")

    if not new_months:
        print("=" * 80)
        print("没有新月份需要处理，DWD 已是最新，程序退出")
        print("=" * 80)
        spark.stop()
        return

    # ============================================================
    # 3. 增量处理每个新月份
    # ============================================================

    for year_month, path in new_months:

        print("=" * 80)
        print(f"处理月份：{year_month}")
        print("=" * 80)

        # --------------------------------------------------------
        # 3.1 读取该月原始数据并清洗
        # --------------------------------------------------------

        raw_df = spark.read.parquet(path)

        month_df = clean_dwd(raw_df)

        print("该月清洗后数据量：", month_df.count())

        # --------------------------------------------------------
        # 3.2 找出该月数据覆盖的 trip_date
        # --------------------------------------------------------

        month_dates = common.dates_to_str(
            month_df
            .select("trip_date")
            .distinct()
            .rdd.flatMap(lambda r: r)
            .collect()
        )

        # 与该月数据重叠的、DWD 已有的日期（跨月边界，如 2025-02-01）
        overlap_dates = month_dates & existing_dates

        # 该月独有的新日期
        only_new_dates = month_dates - existing_dates

        print(
            f"  新增日期：{len(only_new_dates)} 个，"
            f"重叠日期：{len(overlap_dates)} 个"
        )

        # --------------------------------------------------------
        # 3.3 组装要写入的数据
        #
        # 关键点：跨月边界日期（例如 2025-02-01 会同时出现在
        # 2025-01 和 2025-02 两个文件里）。
        #
        # 如果直接跳过重叠日期，2025-02 文件里属于 2-01 的数据
        # 就会丢失，导致 DWD 里 2025-02-01 分区不完整。
        #
        # 正确做法：重叠日期 = DWD 旧数据 + 本月新数据，合并去重。
        # 非重叠日期 = 只写本月数据。
        # --------------------------------------------------------

        parts = []

        if overlap_dates:
            # DWD 里已有的重叠日期数据
            old_part = (
                spark.read.parquet(common.DWD_PATH)
                .filter(
                    col("trip_date")
                    .isin(sorted(overlap_dates))
                )
            )
            parts.append(old_part)

        if overlap_dates:
            parts.append(
                month_df.filter(
                    col("trip_date")
                    .isin(sorted(overlap_dates))
                )
            )

        if only_new_dates:
            parts.append(
                month_df.filter(
                    col("trip_date")
                    .isin(sorted(only_new_dates))
                )
            )

        if parts:

            write_df = parts[0]

            for part in parts[1:]:

                # 注意：不能用 union()（按列位置对齐）。
                #
                # DWD 读出来的 trip_date 是分区列，Spark 读 parquet 时
                # 会自动把它放到 schema 最后；而新月份数据的 trip_date
                # 是在加工过程中排在中位的。两边列顺序不同，
                # union 按位置对齐会错位（trip_date 对到 pickup_hour），
                # 报 INCOMPATIBLE_COLUMN_TYPE。
                #
                # unionByName() 按"列名"对齐，不关心列顺序。
                write_df = write_df.unionByName(part)

            # 防止合并后出现完全相同的行
            write_df = write_df.dropDuplicates()

        else:
            write_df = month_df

        # --------------------------------------------------------
        # 3.4 关键：先把 write_df 物化到缓存，再删除旧分区
        #
        # Spark 是惰性执行的：上面的 read / filter / unionByName /
        # dropDuplicates 都不会真正读数据，直到遇到 action
        # （count / write）才执行。
        #
        # 如果直接先 delete_partitions 删掉旧分区文件，
        # 那么 write 执行时再去读这些文件（old_part 的 lineage
        # 仍指向它们）就会报 SparkFileNotFoundException：
        #   trip_date=2025-01-31/part-xxx.parquet does not exist
        #
        # 解决办法：先 cache 并触发一次 count() 强制物化，
        # 让数据进入缓存；删除分区后再写，write 从缓存读取，
        # 不再依赖磁盘上被删掉的文件。
        # --------------------------------------------------------

        if parts:
            write_df.cache()
            write_df.count()

        # --------------------------------------------------------
        # 3.5 分区级覆盖写入
        #
        # 先删除受影响的旧分区，再 append 写入新分区。
        # 这样被重算的日期不会残留旧数据。
        # --------------------------------------------------------

        affected_dates = only_new_dates | overlap_dates

        print("受影响分区数：", len(affected_dates))

        common.delete_partitions(
            common.DWD_PATH, "trip_date", affected_dates
        )

        (
            write_df
            .write
            .mode("append")
            .partitionBy("trip_date")
            .parquet(common.DWD_PATH)
        )

        # 释放缓存
        if parts:
            write_df.unpersist()

        # 记录本月的日期已进入 DWD，供下一个月份判断用
        existing_dates |= month_dates

        print(f"{year_month} 增量写入完成")

    # ============================================================
    # 4. 汇总
    # ============================================================

    print("=" * 80)
    print("DWD 增量 ETL 完成")
    print("=" * 80)
    print("DWD 分区数：",
          len(common.list_partitions(common.DWD_PATH, "trip_date")))

    spark.stop()


if __name__ == "__main__":
    main()
