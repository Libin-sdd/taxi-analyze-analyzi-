# NYC Taxi PySpark 数据开发项目 —— 项目上下文与续作指南

> 用途：用于在新的 ChatGPT 对话窗口中恢复当前数据开发项目上下文。新窗口读取本文档后，应从“当前进度”和“下一步”继续，不需要重新从头设计项目。

## 一、项目目标

正在学习并实践一个比较完整的：

**PySpark + 数据仓库 + ETL + Spark SQL 数据开发项目**

使用 NYC Taxi（纽约出租车）公开数据作为数据集。

目标：
- 学习数据开发
- 学习 PySpark
- 学习 Spark SQL
- 学习数仓分层
- 学习 ETL
- 学习窗口函数
- 学习数据质量分析
- 学习业务指标设计
- 后续整理到 GitHub
- 后续作为简历项目

## 二、当前机器环境

用户目前没有 Hadoop 集群、没有 YARN 集群，使用本机 Spark `local[*]` 模式运行。

当前环境：

```text
Spark 3.5.4
Scala 2.12.18
Java/OpenJDK 11.0.31
Spark 路径：/opt/spark
SPARK_HOME：/opt/spark
```

已经确认：

```bash
spark-submit 03_spark_sql.py
```

可以正常运行。

## 三、项目数据

当前使用：

```text
yellow_tripdata_2025-01.parquet
```

原始数据量：

```text
3,475,226
```

主要字段：

```text
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
passenger_count
trip_distance
RatecodeID
store_and_fwd_flag
PULocationID
DOLocationID
payment_type
fare_amount
extra
mta_tax
tip_amount
tolls_amount
improvement_surcharge
total_amount
congestion_surcharge
Airport_fee
cbd_congestion_fee
```

## 四、项目目录

项目根目录类似：

```text
nyc_taxi_data_engineering/
│
├── data/
│   ├── raw/
│   │   └── yellow_tripdata_2025-01.parquet
│   ├── processed/
│   └── warehouse/
│       ├── dwd/
│       │   └── dwd_taxi_trip/
│       ├── dws/
│       │   ├── dws_daily_taxi/
│       │   ├── dws_hourly_taxi/
│       │   └── dws_location_taxi/
│       └── ads/
│
├── jobs/
│   ├── 02_taxi_dwd.py
│   ├── 03_data_quality.py
│   ├── 04_quality_analysis.py
│   ├── 05_distribution_analysis.py
│   ├── 06_outlier_analysis.py
│   ├── 03_dws_daily.py
│   ├── 04_dws_hourly.py
│   └── 05_dws_location.py
│
└── README.md
```

实际文件可能略有差异，以机器上的当前目录为准。

## 五、已经完成的流程

```text
原始数据
   ↓
数据质量分析
   ↓
异常值分析
   ↓
DWD 明细层
   ↓
DWS 每日指标
   ↓
DWS 每小时指标
   ↓
准备进入区域分析
```

尚未完成：

```text
区域维表关联
DWS 完整设计
ADS
最终报表
README
性能优化
```

## 六、数据质量分析

### 1. 原始数据量

```text
3,475,226
```

### 2. NULL

以下字段各有 540,149 条 NULL：

```text
passenger_count
RatecodeID
store_and_fwd_flag
congestion_surcharge
Airport_fee
```

主要字段中以下字段没有 NULL：

```text
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
trip_distance
PULocationID
DOLocationID
payment_type
fare_amount
total_amount
cbd_congestion_fee
```

重要结论：

**不能因为 `passenger_count` NULL 就直接删除这些数据。**

这些记录仍具有时间、上下车区域、距离、金额等业务信息。

## 七、异常值分析

### trip_distance

```text
总量：3475226
distance_zero      = 90893
distance_negative  = 0
min = 0.0
max = 276423.57
avg = 5.855126178843539
```

分布：

```text
0          90893
0~5      2861548
5~10      288453
10~20     206043
20~50      27756
50~100       371
100+        162
```

分位数：

```text
25% = 0.98
50% = 1.67
75% = 3.10
max = 276423.57
```

第一版 DWD 清洗规则：

```text
trip_distance > 0
AND trip_distance <= 100
```

### fare_amount

```text
总量：3475226
fare_negative = 144118
fare_zero     = 1398
min = -900.0
max = 863372.12
avg = 17.08180276045484
```

分布：

```text
负数        144118
0~10       1169454
10~20      1337036
20~50       641035
50~100      174136
100+          8588
```

当前不因为 `fare_amount < 0` 就删除，因为负金额可能具有退款、冲正等业务意义。

### total_amount

```text
总量：3475226
total_negative = 63037
total_zero     = 559
min = -901.0
max = 863380.37
avg = 25.611291697280986
```

分布：

```text
负数        63037
0~20      1682522
20~50     1415449
50~100     269047
100~500     44533
500+           82
```

当前 DWD 不因为 `total_amount < 0` 而删除。后续做收入指标时再明确业务口径。

## 八、当前 DWD 清洗规则

第一版 DWD：

```text
1. tpep_pickup_datetime IS NOT NULL
2. tpep_dropoff_datetime IS NOT NULL
3. tpep_dropoff_datetime > tpep_pickup_datetime
4. trip_distance > 0
5. trip_distance <= 100
6. PULocationID IS NOT NULL
7. DOLocationID IS NOT NULL
8. trip_duration_minutes > 0
9. trip_duration_minutes <= 1440
10. passenger_count NULL 暂时保留
11. fare_amount < 0 暂时保留
12. total_amount < 0 暂时保留
```

新增字段：

```text
trip_date
pickup_hour
trip_duration_minutes
```

计算逻辑：

```text
trip_date = tpep_pickup_datetime 的日期
pickup_hour = tpep_pickup_datetime 的小时
trip_duration_minutes =
    (dropoff_timestamp - pickup_timestamp) / 60
```

DWD 使用：

```python
.partitionBy("trip_date")
.parquet(output_path)
```

保存：

```text
data/warehouse/dwd/dwd_taxi_trip
```

## 九、DWD 实际运行结果

运行：

```bash
spark-submit jobs/02_taxi_dwd.py
```

结果：

```text
ODS 原始数据量：3475226
基础清洗完成
清洗后数据量：3382812
过滤数据量：92414

DWD 最终数据量：3382798
最终过滤数据量：92428
```

因此：

```text
ODS = 3,475,226
DWD = 3,382,798
```

最终保留约：

```text
97.34%
```

## 十、日期口径的重要说明

虽然数据文件名是：

```text
yellow_tripdata_2025-01.parquet
```

但 DWS 结果中出现：

```text
2024-12-31 | 21
```

这是因为 `trip_date` 根据真实的 `tpep_pickup_datetime` 计算。

项目当前决定：

**DWD 尽量保留真实业务数据，不因为文件名月份而删除。**

如果 ADS 统计 2025 年 1 月，则使用：

```sql
WHERE trip_date >= '2025-01-01'
  AND trip_date < '2025-02-01'
```

进行业务口径过滤。

## 十一、DWS 每日运营指标

程序：

```text
jobs/03_dws_daily.py
```

读取：

```text
data/warehouse/dwd/dwd_taxi_trip
```

按：

```text
trip_date
```

聚合：

```text
trip_count
total_revenue
avg_revenue
avg_distance
avg_duration
min_duration
max_duration
```

保存：

```text
data/warehouse/dws/dws_daily_taxi
```

部分结果：

```text
2024-12-31 | 21    | 589.17     | 28.06 | 3.66 | 16.81 | 1.07 | 96.25
2025-01-01 | 88604 | 2236469.12 | 25.24 | 3.99 | 15.66 | 0.03 | 1437.17
2025-01-02 | 83216 | 2318190.71 | 27.86 | 3.75 | 16.89 | 0.02 | 1438.42
2025-01-03 | 89489 | 2385912.36 | 26.66 | 3.42 | 16.08 | 0.05 | 1438.90
2025-01-04 | 96564 | 2450261.09 | 25.37 | 3.28 | 15.11 | 0.02 | ...
```

## 十二、DWS 每小时运营指标

程序：

```text
jobs/04_dws_hourly.py
```

按：

```text
pickup_hour
```

聚合：

```text
trip_count
total_revenue
avg_revenue
avg_distance
avg_duration
```

保存：

```text
data/warehouse/dws/dws_hourly_taxi
```

完整结果：

```text
0  | 91998  | 2321187.61 | 25.23 | 3.68 | 13.93
1  | 63329  | 1471720.06 | 23.24 | 3.26 | 13.02
2  | 43006  | 947446.67  | 22.03 | 3.13 | 12.80
3  | 27688  | 621409.67  | 22.44 | 3.47 | 12.79
4  | 19361  | 538648.59  | 27.82 | 4.76 | 14.34
5  | 21973  | 693408.97  | 31.56 | 5.73 | 16.12
6  | 49208  | 1332590.61 | 27.08 | 4.56 | 15.80
7  | 101284 | 2562868.10 | 25.30 | 3.60 | 15.77
8  | 139512 | 3363953.46 | 24.11 | 2.99 | 15.64
9  | 141014 | 3447431.21 | 24.45 | 2.91 | 15.15
10 | 146395 | 3628043.75 | 24.78 | 2.97 | 15.04
11 | 158047 | 3877668.26 | 24.53 | 2.87 | 15.41
12 | 173019 | 5140994.15 | 29.71 | 2.96 | 15.30
13 | 183586 | 4646805.07 | 25.31 | 3.06 | 15.78
14 | 199561 | 5196367.72 | 26.04 | 3.20 | 16.65
15 | 210682 | 5489182.67 | 26.05 | 3.20 | 16.85
16 | 208823 | 5833680.06 | 27.94 | 3.24 | 16.81
17 | 237420 | 6222484.35 | 26.21 | 2.88 | 15.88
18 | 246186 | 6127530.56 | 24.89 | 2.70 | 14.47
19 | 208632 | 5328564.41 | 25.54 | 2.99 | 14.08
20 | 192877 | 4861415.91 | 25.20 | 3.25 | 13.78
21 | 203832 | 5133215.49 | 25.18 | 3.28 | 13.92
22 | 180331 | 4567395.57 | 25.33 | 3.41 | 14.18
23 | 135034 | 3475249.58 | 25.74 | 3.75 | 14.12
```

## 十三、小时 Top 5

```text
18:00 → 246186
17:00 → 237420
15:00 → 210682
16:00 → 208823
19:00 → 208632
```

业务结论：

**18:00 订单量最高。**

但：

**17:00 总收入最高。**

17:00：

```text
trip_count = 237420
total_revenue = 6222484.35
avg_revenue = 26.21
```

18:00：

```text
trip_count = 246186
total_revenue = 6127530.56
avg_revenue = 24.89
```

重要结论：

> 订单量最高的小时不一定是收入最高的小时。

## 十四、当前正在进行：DWS 区域分析

目标：

按照：

```text
PULocationID
```

统计：

```text
订单量
总收入
平均收入
平均距离
```

并使用：

```text
row_number()
rank()
dense_rank()
Window
```

计算排名。

目标输出：

```text
PULocationID
trip_count
total_revenue
avg_revenue
avg_distance
row_number
rank
dense_rank
```

查看：

```text
上车区域订单量 Top 20
```

保存：

```text
data/warehouse/dws/dws_location_taxi
```

## 十五、当前区域分析程序

程序：

```text
jobs/05_dws_location.py
```

核心逻辑：

```python
location_df = (
    df
    .groupBy("PULocationID")
    .agg(
        count("*").alias("trip_count"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_revenue"),
        round(avg("trip_distance"), 2).alias("avg_distance")
    )
)
```

Window：

```python
window_spec = Window.orderBy(
    location_df.trip_count.desc()
)
```

然后：

```python
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
```

运行：

```bash
spark-submit jobs/05_dws_location.py
```

**当前下一步就是运行这个程序，并把“上车区域订单量 Top 20”结果发回来。**

## 十六、为什么学习 Window

`row_number()`：

```text
A 1000 → 1
B 1000 → 2
C 800  → 3
```

一定连续编号。

`rank()`：

```text
A 1000 → 1
B 1000 → 1
C 800  → 3
```

相同排名会跳号。

`dense_rank()`：

```text
A 1000 → 1
B 1000 → 1
C 800  → 2
```

相同排名不跳号。

后续重点学习：

```text
Window
partitionBy()
orderBy()
row_number()
rank()
dense_rank()
```

尤其要进一步实现：

```text
每个区域 Top N
每天 Top N
每小时 Top N
```

## 十七、后续区域维表

区域分析完成后，将：

```text
PULocationID
DOLocationID
```

关联 NYC Taxi Zone 维表。

最终从：

```text
PULocationID = 132
```

变成类似：

```text
JFK Airport
```

等实际区域名称。

目标：

```text
区域排名
区域名称
订单量
收入
平均订单金额
平均距离
```

## 十八、后续 ADS

最终设计：

```text
data/warehouse/ads/
├── ads_daily_report
├── ads_hourly_report
├── ads_location_top10
└── ads_taxi_summary
```

最终从 DWS 产生面向业务的报表。

## 十九、最终项目架构

```text
                   NYC Taxi
                       │
                       ↓
                Raw Parquet
                       │
                       ↓
                ┌────────────┐
                │    ODS     │
                │ 原始数据层 │
                └────────────┘
                       │
                       ↓
                数据质量分析
                       │
                       ↓
                ┌────────────┐
                │    DWD     │
                │ 明细数据层 │
                └────────────┘
                       │
          ┌────────────┼────────────┐
          ↓            ↓            ↓
       每日统计      小时统计      区域统计
          │            │            │
          └────────────┼────────────┘
                       ↓
                ┌────────────┐
                │    DWS     │
                │ 汇总数据层 │
                └────────────┘
                       │
                       ↓
                维表关联/业务口径
                       │
                       ↓
                ┌────────────┐
                │    ADS     │
                │ 应用数据层 │
                └────────────┘
                       │
                       ↓
                报表 / BI / 分析
```

## 二十、项目设计原则

### 原则 1：不要为了清洗而清洗

不要简单地：

```text
NULL → 删除
负数 → 删除
```

必须考虑业务含义。

### 原则 2：DWD 尽量保留业务明细

DWD 负责：

```text
清洗
标准化
字段加工
异常过滤
```

### 原则 3：DWS 做业务汇总

例如：

```text
每天订单量
每天收入
每小时订单量
区域订单量
```

### 原则 4：ADS 负责最终业务口径

例如：

```text
2025年1月收入
有效订单
Top 10 区域
高峰时段
```

### 原则 5：没有集群也可以完成项目

当前：

```python
.master("local[*]")
```

即可完成：

```text
ODS
DWD
DWS
ADS
```

以后有 Hadoop/YARN 后，再迁移到：

```text
HDFS
YARN
Spark Cluster
```

## 二十一、学习方式要求

用户已经有一定 Spark SQL 基础，之前学习过：

```text
Spark
Spark SQL
PySpark
Window
row_number
rank
dense_rank
lag
lead
```

之前写过：

```text
03_spark_sql.py
```

后续讲解应该：

- 不要只给代码
- 每一步解释为什么做
- 解释代码对应的 SQL
- 解释数仓为什么这么设计
- 解释业务指标含义
- 逐步推进
- 每一步运行后分析结果
- 不要一次性把整个项目全部写完

## 二十二、最终 GitHub 项目目标

最终整理为：

```text
NYC Taxi Data Engineering
│
├── README.md
├── data/
├── jobs/
├── sql/
├── docs/
└── ...
```

README 最终包含：

```text
1. 项目背景
2. 数据来源
3. 技术栈
4. 数据架构
5. ODS/DWD/DWS/ADS
6. 数据清洗规则
7. 数据质量分析
8. 核心指标
9. Spark SQL
10. Window Functions
11. 项目运行方式
12. 项目结果
13. 性能优化
14. 后续可扩展方向
```

最终体现：

```text
PySpark
Spark SQL
ETL
数仓分层
数据质量
窗口函数
分区
Parquet
业务指标
维表关联
Top N
数据分析
```

而不是只有：

```text
读取数据
→ groupBy
→ show()
```

## 二十三、当前状态

```text
[完成] 数据集下载
[完成] 项目目录
[完成] Spark 本地环境
[完成] 原始数据读取
[完成] 数据质量分析
[完成] 异常值分析
[完成] DWD 明细层
[完成] DWD 分区
[完成] DWS 每日指标
[完成] DWS 每小时指标

[进行中] DWS 区域指标
[待完成] Zone 维表
[待完成] 区域 Top N
[待完成] 更多 DWS
[待完成] ADS
[待完成] 最终报表
[待完成] 性能优化
[待完成] GitHub README
[待完成] 简历项目总结
```

## 二十四、新窗口恢复指令

新窗口上传本文档后，可以直接说：

> 继续 NYC Taxi PySpark 数据开发项目。我已经完成 ODS、DWD、DWS 每日和每小时指标，请按照上下文从下一步继续带我做。

如果还没有运行区域程序：

```bash
spark-submit jobs/05_dws_location.py
```

然后把：

```text
上车区域订单量 Top 20
```

结果发回来。

如果已经运行，则直接分析区域 Top 20 结果，不要重新从数据下载开始。
