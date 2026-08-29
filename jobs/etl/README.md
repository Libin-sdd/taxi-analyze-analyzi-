# NYC Taxi ETL 增量改造说明

> 本文档说明 2026-08-29 完成的 ETL 增量改造：背景、设计、改动清单、运行方式和验证方法。
> 相关脚本：`jobs/etl/` 目录。

---

## 一、背景：为什么从全量改为增量

改造前，整条 ETL 是**全量（FULL LOAD）**模式：

```text
raw/yellow_tripdata_2025-01.parquet   ← 脚本里写死只处理 1 个月
  ↓ 02_taxi_dwd.py    mode("overwrite") + partitionBy("trip_date")  → 整层推倒重建
  ↓ 03/04/05 DWS      读全量 DWD → groupBy → overwrite
  ↓ 07 location_zone  join 维表 → overwrite
  ↓ 09~13 ADS         读 DWS → overwrite
  ↓ 16_load_mysql     TRUNCATE + INSERT
```

全量模式的问题：

1. **DWD 明细层每次全量重建**：数据量越大越慢。
2. **DWS 每次都扫描全量 DWD**：即使只新增一天，也要把整张 DWD 重新聚合。
3. **脚本写死月份**：raw 目录新增文件（目前已从 1 个月增加到 7 个月）不会被自动处理。
4. **没有进度记录**：程序不知道自己处理到哪，重复跑会浪费时间，甚至产生重复数据。

---

## 二、增量改造总体设计：大表增量、小表全量

数仓实践中，**并不是所有层都适合增量**，核心原则是：

> **明细大表（DWD）增量是关键；汇总小表（DWS/ADS）全量重算更简单可靠。**

| 层 | 数据特征 | 增量策略 |
|---|---|---|
| raw 原始文件 | 按月分文件 | **自动扫描**发现新月份，与 DWD 分区对比，只处理缺失月份 |
| DWD 明细层 | 数据量最大、持续增长 | **分区级覆盖增量**：只处理新月份；跨月边界日期做合并 |
| DWS daily 每日 | 按天聚合 | **按 trip_date 分区增量**：只重算「新增 + 被更新」的日期 |
| DWS hourly / location | 全局聚合（24 小时 / 263 区域） | 全量重算（结果只有几十~几百行，重算成本低且绝对正确） |
| ADS 报表 | 行数极少 | 全量重算 |
| MySQL 报表库 | 报表小表 | **按主键 upsert** 增量写入 |

---

## 三、改动文件清单

### 新增文件

| 文件 | 说明 |
|---|---|
| `etl_common.py` | ETL 公共工具模块：统一路径、SparkSession、raw 扫描、分区列表/删除、月份覆盖判断 |

### 修改文件

| 文件 | 改动 | 增量核心逻辑 |
|---|---|---|
| `02_taxi_dwd.py` | 重写 | 自动扫描 raw 发现新月份；只处理未覆盖月份；跨月边界日期合并去重；分区级覆盖写入 |
| `03_dws_daily.py` | 重写 | 按 `trip_date` 分区增量，只重算「DWD 新增 + 被更新」的日期；兼容旧的非分区数据 |
| `16_load_mysql.py` | 修改 | TRUNCATE+INSERT 改为按主键 `INSERT ... ON DUPLICATE KEY UPDATE`（upsert） |

### 未改动（设计上保留全量重算）

`04_dws_hourly.py`、`05_dws_location.py`、`07_dws_location_zone.py`、`08~13`（ADS 系列）

> 原因：这些是全局聚合小表，全量重算成本低、逻辑简单、绝对正确。
> 若改成累计计数器式增量，需要同时维护"加"与"减"逻辑，复杂度高且易出错，收益却很小。

---

## 四、各脚本增量机制详解

### 1. `etl_common.py` — 公共模块

核心函数：

- `list_raw_files()`：扫描 `data/raw/` 下所有 `yellow_tripdata_YYYY-MM.parquet`，自动发现新月份。
- `list_partitions(base_path, key)`：只列目录、不读数据，快速拿到已处理的分区集合。
- `delete_partitions(base_path, key, values)`：删除指定分区目录 —— "分区级覆盖"的关键。
- `month_is_covered(year_month, existing_dates)`：判断某月文件是否已被 DWD 完全处理（该月每一天的 `trip_date` 分区都存在）。
- `mtime_of(...)`：比较分区目录修改时间，判断 DWD 分区是否被重算过。

### 2. `02_taxi_dwd.py` — DWD 明细层增量（核心）

处理流程：

1. 扫描 raw 全部月份文件。
2. 与 DWD 已有 `trip_date` 分区对比，找出**未覆盖的月份**。
3. 对每个新月份：
   - 读取该月原始数据并清洗（清洗规则与原来一致）。
   - 计算该月数据覆盖的 `trip_date` 集合。
   - 与 DWD 已有分区求交集 —— 得到**跨月边界日期**（例如 `2025-02-01` 同时存在于 1 月和 2 月的文件里）。
   - **合并逻辑**：边界日期 = DWD 旧数据 ∪ 本月新数据，去重；非边界日期只写本月数据。
   - **分区级覆盖**：先删除受影响的旧分区，再 `append` 写入新分区。

> 关键点：如果直接跳过"已在 DWD 中的边界日期"，会丢失新文件里该日期的数据，
> 导致该日期分区不完整。合并逻辑保证了分区数据的完整性。

### 3. `03_dws_daily.py` — DWS 每日指标增量

需要重算的日期 = 两类：

- **新增日期**：DWD 有、DWS 没有。
- **被更新日期**：DWD 分区比 DWS 分区新（说明 DWD 该日期被重算过，如跨月边界被补充）。

判断方式只是对比分区目录 + 文件修改时间，几乎不消耗计算资源。

读取时用 `filter(col("trip_date").isin(...))`，Spark 会自动做**分区裁剪**，
只扫描需要重算的分区，不碰整张 DWD。

输出同样按 `trip_date` 分区，分区级覆盖写入。

> 兼容旧数据：旧版 DWS daily 是非分区写的平铺文件，脚本检测到后会自动整体重建一次。

### 4. `16_load_mysql.py` — MySQL 增量写入

- 为每个表定义**主键**（`PRIMARY_KEYS` 映射）。
- 写入 SQL 改为 `INSERT ... ON DUPLICATE KEY UPDATE`：
  - 主键不存在 → 插入新行（增量新增）
  - 主键已存在 → 更新整行（增量更新）
- 旧版本创建的表没有主键，脚本会自动 `ALTER TABLE` 补上。
- 单行整体指标表 `ads_overall_metrics` 没有业务主键，保留 TRUNCATE+INSERT 覆盖写。

各表主键：

| 表 | 主键 |
|---|---|
| `dws_daily_taxi` | `trip_date` |
| `dws_hourly_taxi` | `pickup_hour` |
| `dws_location_taxi` / `dws_location_zone` | `PULocationID` |
| `location_trip_top10` / 各 `ads_*_top10` / `ads_location_data_quality` | `PULocationID` |

---

## 五、完整运行流程

### 方式一：一键流水线（推荐）

使用调度脚本 `run_pipeline.py`，按依赖顺序自动执行并校验：

```bash
cd /home/lishaobin/project/数开发/jobs/etl

# 跑全部核心步骤（02→03→04→05→07→08~13→16），失败即停
python run_pipeline.py

# 只跑指定步骤
python run_pipeline.py --steps 02,03

# 跳过指定步骤
python run_pipeline.py --skip 16

# 某步失败后继续执行
python run_pipeline.py --continue-on-error

# 只校验产物是否齐全，不运行
python run_pipeline.py --check
```

运行日志自动保存到 `data/logs/run_pipeline_时间戳.log`。

### 方式二：手动逐步执行

在 `bz_sql` conda 环境中，进入 `jobs/etl/` 目录按顺序执行：

```bash
cd /home/lishaobin/project/数开发/jobs/etl

# 1. DWD 明细层增量（自动处理新月份，耗时较长）
python 02_taxi_dwd.py

# 2. DWS 每日指标增量
python 03_dws_daily.py

# 3. DWS 小时 / 区域 / 区域+维表（全局聚合小表，全量重算）
python 04_dws_hourly.py
python 05_dws_location.py
python 07_dws_location_zone.py

# 4. ADS 报表（全量重算）
python 08_analysis_location.py
python 09_ads_location_revenue_top10.py
python 10_ads_location_avg_revenue_top10.py
python 11_ads_borough_location_top3.py
python 12_ads_location_data_quality.py
python 13_ads_overall_metrics.py

# 5. 加载 MySQL（upsert 增量写入）
python 16_load_mysql.py
```

---

## 六、如何验证增量生效

1. **幂等性验证（最重要）**：跑完一次后，**再跑一次** `python 02_taxi_dwd.py`，
   应输出"没有新月份需要处理，DWD 已是最新"，程序快速退出 —— 说明增量生效、没有重复处理。
2. **再跑一次** `python 03_dws_daily.py`，应输出"DWS 每日指标已是最新，无需更新"。
3. **MySQL 重复加载**：再跑一次 `16_load_mysql.py`，数据量不变（upsert 不会产生重复行）。
4. **数据量对比**：raw 目录现在有 7 个月文件，跑完后 DWD 分区数应从 33 增加到覆盖全部月份。

---

## 七、注意事项

1. **跨月边界日期**：每月文件的第一天 / 最后一天会与相邻月份文件重叠，
   DWD 增量已通过合并去重处理，请勿自行跳过这些日期。
2. **DWD / DWS daily 分区结构**：DWD 按 `trip_date` 分区（原本就有）；
   DWS daily 改为按 `trip_date` 分区（旧数据会自动迁移重建）。
3. **MySQL 主键**：第一次运行 `16_load_mysql.py` 会自动为已有表补充主键；
   如果提示"主键已存在"属于正常现象（脚本已忽略）。
4. **新增原始数据**：只要把新的 `yellow_tripdata_YYYY-MM.parquet` 放进
   `data/raw/`，重新运行 `02_taxi_dwd.py` 即会自动处理，无需改脚本。
5. **运行环境**：本机 Spark 为 `local[*]` 模式，Spark 任务需要 JVM 正常启动。
   若在受限制的沙箱环境中运行，需要放行 JVM 对 `/proc` 的访问。
