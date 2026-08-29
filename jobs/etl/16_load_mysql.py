from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType
)

import mysql.connector
from getpass import getpass


# ============================================================
# 各表主键（增量 upsert 的依据）
#
# 有了主键之后，写入方式从"先 TRUNCATE 再 INSERT"改成
# "INSERT ... ON DUPLICATE KEY UPDATE"：
#   - 主键不存在 -> 插入新行（增量新增）
#   - 主键已存在 -> 更新整行（增量更新）
# 这样下游 MySQL 表永远是最新的，且不会产生重复数据。
#
# 注意：ads_overall_metrics 是单行整体指标，没有业务主键，
# 继续使用覆盖写（TRUNCATE + INSERT）。
# ============================================================

PRIMARY_KEYS = {
    "dws_daily_taxi": ["trip_date"],
    "dws_hourly_taxi": ["pickup_hour"],
    "dws_location_taxi": ["PULocationID"],
    "dws_location_zone": ["PULocationID"],
    "location_trip_top10": ["PULocationID"],
    "ads_location_revenue_top10": ["PULocationID"],
    "ads_location_avg_revenue_top10": ["PULocationID"],
    "ads_borough_location_top3": ["PULocationID"],
    "ads_location_data_quality": ["PULocationID"],
}


# ============================================================
# Spark
# ============================================================

def create_spark():

    return (
        SparkSession.builder
        .appName("NYC Taxi Load MySQL")
        .master("local[2]")
        .getOrCreate()
    )


# ============================================================
# Spark 类型 -> MySQL 类型
# ============================================================

def spark_type_to_mysql(data_type):

    if isinstance(data_type, StringType):
        return "VARCHAR(255)"

    elif isinstance(data_type, IntegerType):
        return "INT"

    elif isinstance(data_type, LongType):
        return "BIGINT"

    elif isinstance(data_type, DoubleType):
        return "DOUBLE"

    elif isinstance(data_type, FloatType):
        return "FLOAT"

    elif isinstance(data_type, DecimalType):
        return "DECIMAL(20,4)"

    elif isinstance(data_type, DateType):
        return "DATE"

    elif isinstance(data_type, TimestampType):
        return "DATETIME"

    elif isinstance(data_type, BooleanType):
        return "BOOLEAN"

    else:
        return "VARCHAR(255)"


# ============================================================
# 打印标题
# ============================================================

def print_title(title):

    print("=" * 80)
    print(title)
    print("=" * 80)


# ============================================================
# 创建 MySQL 表
# ============================================================

def create_mysql_table(
        cursor,
        table_name,
        spark_df,
        primary_key=None
):

    fields = []

    for field in spark_df.schema.fields:

        column_name = field.name
        mysql_type = spark_type_to_mysql(
            field.dataType
        )

        fields.append(
            f"`{column_name}` {mysql_type}"
        )

    # 有主键时，在建表语句里带上 PRIMARY KEY
    if primary_key:
        key_sql = ", ".join(
            f"`{key}`" for key in primary_key
        )
        fields.append(
            f"PRIMARY KEY ({key_sql})"
        )

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {", ".join(fields)}
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4;
    """

    cursor.execute(create_sql)


# ============================================================
# 给已有表补充主键
#
# 旧版本建的表没有主键，导致无法 upsert。
# 这里尝试 ALTER TABLE 补上；如果主键已存在会报错，忽略即可。
# ============================================================

def ensure_primary_key(
        cursor,
        table_name,
        primary_key
):

    if not primary_key:
        return

    key_sql = ", ".join(
        f"`{key}`" for key in primary_key
    )

    try:
        cursor.execute(
            f"ALTER TABLE `{table_name}` "
            f"ADD PRIMARY KEY ({key_sql})"
        )
        print(
            f"  已为 {table_name} 补充主键：{key_sql}"
        )

    except Exception as e:
        # 主键已存在等情况，忽略
        pass


# ============================================================
# 清空表
# ============================================================

def truncate_table(
        cursor,
        table_name
):

    cursor.execute(
        f"TRUNCATE TABLE `{table_name}`"
    )


# ============================================================
# Spark DataFrame -> MySQL
# ============================================================

def load_dataframe_to_mysql(
        spark_df,
        table_name,
        mysql_config
):

    print_title(
        f"开始加载：{table_name}"
    )

    # --------------------------------------------------------
    # 数据量
    # --------------------------------------------------------

    count = spark_df.count()

    print(
        f"数据量：{count}"
    )

    if count == 0:

        print(
            "数据为空，跳过"
        )

        return

    # --------------------------------------------------------
    # 转 Pandas
    #
    # DWS / ADS 都是聚合后的少量数据
    # 可以安全地这样处理
    # --------------------------------------------------------

    pandas_df = spark_df.toPandas()

    # --------------------------------------------------------
    # MySQL 连接
    # --------------------------------------------------------

    connection = mysql.connector.connect(
        host=mysql_config["host"],
        port=mysql_config["port"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
        charset="utf8mb4"
    )

    cursor = connection.cursor()

    # --------------------------------------------------------
    # 主键（upsert 增量写入的依据）
    # --------------------------------------------------------

    primary_key = PRIMARY_KEYS.get(table_name)

    create_mysql_table(
        cursor,
        table_name,
        spark_df,
        primary_key
    )

    if primary_key:
        ensure_primary_key(
            cursor,
            table_name,
            primary_key
        )

    # --------------------------------------------------------
    # 写入 SQL
    #
    # 有主键 -> INSERT ... ON DUPLICATE KEY UPDATE（upsert 增量）
    # 无主键 -> TRUNCATE + INSERT（整体覆盖，例如单行指标表）
    # --------------------------------------------------------

    columns = list(
        pandas_df.columns
    )

    column_sql = ", ".join(
        f"`{column}`"
        for column in columns
    )

    placeholder_sql = ", ".join(
        ["%s"] * len(columns)
    )

    if primary_key:

        # 主键之外的字段，冲突时全部更新
        update_columns = [
            column for column in columns
            if column not in primary_key
        ]

        update_sql = ", ".join(
            f"`{column}` = VALUES(`{column}`)"
            for column in update_columns
        )

        write_sql = f"""
        INSERT INTO `{table_name}`
        ({column_sql})
        VALUES
        ({placeholder_sql})
        ON DUPLICATE KEY UPDATE
        {update_sql}
        """

        print("写入方式：增量 upsert（按主键）")

    else:

        # 没有主键的表（例如单行整体指标），整体覆盖
        truncate_table(
            cursor,
            table_name
        )

        write_sql = f"""
        INSERT INTO `{table_name}`
        ({column_sql})
        VALUES
        ({placeholder_sql})
        """

        print("写入方式：覆盖写（TRUNCATE + INSERT）")

    # --------------------------------------------------------
    # 数据转换
    # --------------------------------------------------------

    rows = []

    for row in pandas_df.itertuples(
            index=False,
            name=None
    ):

        new_row = []

        for value in row:

            # pandas NaN -> None
            if pandas_is_null(value):

                new_row.append(None)

            else:

                # numpy 类型转 Python 类型
                if hasattr(value, "item"):

                    try:
                        value = value.item()

                    except Exception:
                        pass

                new_row.append(value)

        rows.append(
            tuple(new_row)
        )

    # --------------------------------------------------------
    # 批量插入
    # --------------------------------------------------------

    cursor.executemany(
        write_sql,
        rows
    )

    connection.commit()

    print(
        f"MySQL 插入完成：{len(rows)} 条"
    )

    # --------------------------------------------------------
    # 验证
    # --------------------------------------------------------

    cursor.execute(
        f"SELECT COUNT(*) FROM `{table_name}`"
    )

    mysql_count = cursor.fetchone()[0]

    print(
        f"MySQL 数据量：{mysql_count}"
    )

    if mysql_count == count:

        print(
            "✓ 数据量验证通过"
        )

    else:

        print(
            "✗ 数据量不一致"
        )

    cursor.close()

    connection.close()


# ============================================================
# NULL 判断
# ============================================================

def pandas_is_null(value):

    try:

        import pandas as pd

        return pd.isna(value)

    except Exception:

        return False


# ============================================================
# 主函数
# ============================================================

def main():

    # ========================================================
    # MySQL 配置
    # ========================================================

    print_title(
        "MySQL 配置"
    )

    # ========================================================
    # 数据库配置：写死（不再交互输入）
    # ========================================================

    mysql_config = {
        "host": "127.0.0.1",
        "port": 3307,
        "user": "root",
        "password": "FLzx3qcYSyhL9t",
        "database": "nyc_taxi"
    }

    print(
        f"host: {mysql_config['host']}"
    )

    print(
        f"port: {mysql_config['port']}"
    )

    print(
        f"user: {mysql_config['user']}"
    )

    print(
        f"database: {mysql_config['database']}"
    )

    # ========================================================
    # Spark
    # ========================================================

    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    # ========================================================
    # 数据路径
    #
    # 当前脚本：
    #
    # ~/project/数开发/jobs/etl/
    #
    # data：
    #
    # ~/project/数开发/data/
    #
    # 所以：
    #
    # ../../data
    # ========================================================

    data_path = "../../data"

    warehouse_path = (
        f"{data_path}/warehouse"
    )

    dws_path = (
        f"{warehouse_path}/dws"
    )

    ads_path = (
        f"{warehouse_path}/ads"
    )

    # ========================================================
    # 需要加载的数据
    # ========================================================

    tables = [

        # ----------------------------------------------------
        # DWS
        # ----------------------------------------------------

        {
            "path": f"{dws_path}/dws_daily_taxi",
            "table": "dws_daily_taxi"
        },

        {
            "path": f"{dws_path}/dws_hourly_taxi",
            "table": "dws_hourly_taxi"
        },

        {
            "path": f"{dws_path}/dws_location_taxi",
            "table": "dws_location_taxi"
        },

        {
            "path": f"{dws_path}/dws_location_zone",
            "table": "dws_location_zone"
        },

        # ----------------------------------------------------
        # ADS
        # ----------------------------------------------------

        {
            "path": f"{ads_path}/ads_overall_metrics",
            "table": "ads_overall_metrics"
        },

        {
            "path": f"{ads_path}/location_trip_top10",
            "table": "ads_location_trip_top10"
        },

        {
            "path": f"{ads_path}/ads_location_revenue_top10",
            "table": "ads_location_revenue_top10"
        },

        {
            "path": f"{ads_path}/ads_location_avg_revenue_top10",
            "table": "ads_location_avg_revenue_top10"
        },

        {
            "path": f"{ads_path}/ads_borough_location_top3",
            "table": "ads_borough_location_top3"
        },

        {
            "path": f"{ads_path}/ads_location_data_quality",
            "table": "ads_location_data_quality"
        }
    ]

    # ========================================================
    # 开始加载
    # ========================================================

    print_title(
        "开始加载 DWS / ADS 到 MySQL"
    )

    print(
        f"数据库：{mysql_config['database']}"
    )

    print(
        f"数据目录：{warehouse_path}"
    )

    success_count = 0

    # ========================================================
    # 循环加载
    # ========================================================

    for item in tables:

        path = item["path"]

        table_name = item["table"]

        print_title(
            f"读取：{path}"
        )

        # ----------------------------------------------------
        # 判断目录是否存在
        # ----------------------------------------------------

        if not os.path.exists(path):

            print(
                f"⚠️ 路径不存在，跳过：{path}"
            )

            continue

        # ----------------------------------------------------
        # 读取 Parquet
        # ----------------------------------------------------

        df = spark.read.parquet(
            path
        )

        df.printSchema()

        # ----------------------------------------------------
        # 加载 MySQL
        # ----------------------------------------------------

        try:

            load_dataframe_to_mysql(
                df,
                table_name,
                mysql_config
            )

            success_count += 1

        except Exception as e:

            print(
                f"✗ 加载失败：{table_name}"
            )

            print(
                "错误：",
                str(e)
            )

    # ========================================================
    # 总结
    # ========================================================

    print_title(
        "MySQL 加载完成"
    )

    print(
        f"成功加载表数量：{success_count}/{len(tables)}"
    )

    spark.stop()


# ============================================================
# 程序入口
# ============================================================

if __name__ == "__main__":

    import os

    main()