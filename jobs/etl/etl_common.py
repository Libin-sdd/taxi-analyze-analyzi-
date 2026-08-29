"""
ETL 公共工具模块
================
所有 ETL 脚本共享：

- 路径配置（统一管理，避免每个脚本各写各的路径）
- SparkSession 创建
- raw 目录自动扫描（增量数据发现的关键）
- 分区读取 / 分区级删除（"分区级覆盖"的关键）
- 日期 / 月份辅助函数
"""

import os
import re
import shutil
import calendar
from datetime import date

from pyspark.sql import SparkSession


# ============================================================
# 1. 路径配置
#
# 本文件位于 jobs/etl/ 下，所以项目 data 目录是 ../../data
# ============================================================

_PROJECT_DATA = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../data")
)

RAW_PATH = os.path.join(_PROJECT_DATA, "raw")

WAREHOUSE_PATH = os.path.join(_PROJECT_DATA, "warehouse")

DWD_PATH = os.path.join(WAREHOUSE_PATH, "dwd", "dwd_taxi_trip")

DWS_DAILY_PATH = os.path.join(WAREHOUSE_PATH, "dws", "dws_daily_taxi")
DWS_HOURLY_PATH = os.path.join(WAREHOUSE_PATH, "dws", "dws_hourly_taxi")
DWS_LOCATION_PATH = os.path.join(WAREHOUSE_PATH, "dws", "dws_location_taxi")
DWS_LOCATION_ZONE_PATH = os.path.join(WAREHOUSE_PATH, "dws", "dws_location_zone")

ADS_PATH = os.path.join(WAREHOUSE_PATH, "ads")

ZONE_DIM_PATH = os.path.join(
    WAREHOUSE_PATH, "dimension", "NYC_Taxi_Zones_20260828.csv"
)


# ============================================================
# 2. SparkSession
# ============================================================

def create_spark(app_name, master="local[*]"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .getOrCreate()
    )


# ============================================================
# 3. 扫描 raw 原始文件（自动发现新月份）
# ============================================================

def list_raw_files():
    """
    扫描 data/raw/ 目录下的所有月份文件。

    返回 [(year_month, 绝对路径), ...]，按月份升序。

    例如：
        [("2025-01", ".../data/raw/yellow_tripdata_2025-01.parquet"), ...]

    之后只要 raw 目录里多了新文件，这里会自动发现。
    """
    files = []
    pattern = re.compile(
        r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$"
    )

    for name in os.listdir(RAW_PATH):
        match = pattern.match(name)
        if match:
            year_month = f"{match.group(1)}-{match.group(2)}"
            files.append(
                (year_month, os.path.join(RAW_PATH, name))
            )

    files.sort(key=lambda x: x[0])

    return files


# ============================================================
# 4. 分区读取 / 删除（分区级覆盖的关键）
# ============================================================

def list_partitions(base_path, key):
    """
    列出某个分区目录下已有的分区值。

    参数：
        base_path  分区目录，例如 data/warehouse/dwd/dwd_taxi_trip
        key        分区字段名，例如 "trip_date" / "year_month"

    返回：
        set(值)，例如 {"2025-01-01", "2025-01-02", ...}

    注意：只是列目录，不读取任何数据，速度极快。
    """
    values = set()

    if not os.path.isdir(base_path):
        return values

    prefix = key + "="

    for name in os.listdir(base_path):
        if name.startswith(prefix):
            values.add(name[len(prefix):])

    return values


def delete_partitions(base_path, key, values):
    """
    删除指定分区目录。

    这就是"分区级覆盖"：
        先把旧分区目录删掉，再用 mode("append") 写入新数据。
    旧的、没受影响的分区目录保持不动。

    这样既做到增量，又保证被重算的日期不会残留旧数据。
    """
    if not values:
        return

    for value in values:
        partition_dir = os.path.join(
            base_path, f"{key}={value}"
        )

        if os.path.isdir(partition_dir):
            shutil.rmtree(partition_dir)
            print(f"    删除旧分区：{key}={value}")


def is_partitioned(base_path, key):
    """
    判断一个目录是否已经是按 key 分区写入的。
    用于兼容旧版本（非分区写入）的目录，需要整体重建一次。
    """
    if not os.path.isdir(base_path):
        return True

    prefix = key + "="

    for name in os.listdir(base_path):
        if name.startswith(prefix):
            return True

    # 目录存在但没有分区目录 → 旧的非分区写法
    return False


def mtime_of(base_path, key, value):
    """
    获取某个分区目录的最后修改时间。

    用途：判断 DWD 里某个日期分区是否比 DWS 更新。
    如果 DWD 分区被重算过，目录修改时间会变新，
    下游 DWS 就知道这个日期需要跟着重算。
    """
    partition_dir = os.path.join(
        base_path, f"{key}={value}"
    )

    if os.path.isdir(partition_dir):
        return os.path.getmtime(partition_dir)

    return 0.0


# ============================================================
# 5. 日期 / 月份辅助
# ============================================================

def dates_to_str(values):
    """
    把 datetime.date / Timestamp / str 统一转成 'YYYY-MM-DD' 字符串。

    Spark 读取分区 parquet 后，trip_date 是 DateType，
    collect() 出来是 datetime.date 对象。
    """
    result = set()

    for value in values:
        if isinstance(value, date):
            result.add(value.strftime("%Y-%m-%d"))
        else:
            # Timestamp 形如 "2025-01-01 00:00:00"，截取前 10 位
            result.add(str(value)[:10])

    return result


def month_days(year_month):
    """
    返回某个月的所有日期。

    例如：
        month_days("2025-02")
        -> ["2025-02-01", "2025-02-02", ..., "2025-02-28"]
    """
    year = int(year_month[:4])
    month = int(year_month[5:7])

    last_day = calendar.monthrange(year, month)[1]

    return [
        f"{year_month}-{day:02d}"
        for day in range(1, last_day + 1)
    ]


def month_is_covered(year_month, existing_dates):
    """
    判断一个月的原始文件是否已经被 DWD 完全处理。

    规则：
        DWD 分区里已经包含该月"每一天"的 trip_date。

    例如 2025-02 月文件已处理完毕的条件是：
        DWD 中存在 2025-02-01 ~ 2025-02-28 全部 28 个分区。

    注意：
        跨月边界日期（上月最后一天 / 下月第一天）不属于该月，
        由 02 脚本里的"合并逻辑"单独处理，不影响这里的判断。
    """
    return all(
        day in existing_dates
        for day in month_days(year_month)
    )
