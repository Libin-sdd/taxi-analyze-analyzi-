from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import os
import matplotlib.pyplot as plt
import pandas as pd


def print_title(title):
    print("=" * 80)
    print(title)
    print("=" * 80)


def main():

    # ============================================================
    # 1. Spark
    # ============================================================

    spark = (
        SparkSession.builder
        .appName("NYC Taxi Visualization")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ============================================================
    # 2. 路径
    #
    # 当前脚本：
    # ~/project/数开发/jobs/etl/14_visualization.py
    #
    # 所以 ../../data/warehouse/... 是正确的
    # ============================================================

    base_path = "../../data"

    warehouse_path = os.path.join(
        base_path,
        "warehouse"
    )

    ads_path = os.path.join(
        warehouse_path,
        "ads"
    )

    dws_path = os.path.join(
        warehouse_path,
        "dws"
    )

    visualization_path = os.path.join(
        base_path,
        "visualization"
    )

    # 创建可视化目录
    os.makedirs(
        visualization_path,
        exist_ok=True
    )

    print_title("NYC Taxi 数据可视化分析")

    print("ADS 路径：", ads_path)
    print("DWS 路径：", dws_path)
    print("可视化输出路径：", visualization_path)

    # ============================================================
    # 3. 读取 DWS：小时运营指标
    # ============================================================

    print_title("读取 DWS 小时运营指标")

    hourly_path = os.path.join(
        dws_path,
        "dws_hourly_taxi"
    )

    hourly_df = spark.read.parquet(
        hourly_path
    )

    hourly_df.show(
        24,
        truncate=False
    )

    # ============================================================
    # 4. 24小时订单量趋势
    # ============================================================

    print_title("生成：24小时订单量趋势")

    hourly_pd = (
        hourly_df
        .orderBy("pickup_hour")
        .select(
            "pickup_hour",
            "trip_count"
        )
        .toPandas()
    )

    plt.figure(figsize=(12, 6))

    plt.plot(
        hourly_pd["pickup_hour"],
        hourly_pd["trip_count"],
        marker="o"
    )

    plt.title(
        "NYC Taxi - Hourly Trip Count"
    )

    plt.xlabel(
        "Pickup Hour"
    )

    plt.ylabel(
        "Trip Count"
    )

    plt.xticks(
        range(24)
    )

    plt.grid(
        True,
        alpha=0.3
    )

    plt.tight_layout()

    hourly_output = os.path.join(
        visualization_path,
        "hourly_trip_count.png"
    )

    plt.savefig(
        hourly_output,
        dpi=150
    )

    plt.close()

    print("保存成功：")
    print(hourly_output)

    # ============================================================
    # 5. 读取 ADS：区域订单量 Top10
    # ============================================================

    print_title("读取 ADS 区域订单量 Top10")

    location_trip_path = os.path.join(
        ads_path,
        "location_trip_top10"
    )

    location_trip_df = spark.read.parquet(
        location_trip_path
    )

    location_trip_df.show(
        10,
        truncate=False
    )

    # ============================================================
    # 6. 区域订单量 Top10
    # ============================================================

    print_title("生成：区域订单量 Top10")

    trip_pd = (
        location_trip_df
        .orderBy(
            col("trip_count").desc()
        )
        .select(
            "Zone",
            "trip_count"
        )
        .toPandas()
    )

    # matplotlib 横向柱状图需要反转顺序
    trip_pd = trip_pd.sort_values(
        "trip_count"
    )

    plt.figure(figsize=(12, 7))

    plt.barh(
        trip_pd["Zone"],
        trip_pd["trip_count"]
    )

    plt.title(
        "NYC Taxi - Top 10 Pickup Zones by Trip Count"
    )

    plt.xlabel(
        "Trip Count"
    )

    plt.ylabel(
        "Zone"
    )

    plt.tight_layout()

    trip_output = os.path.join(
        visualization_path,
        "location_trip_top10.png"
    )

    plt.savefig(
        trip_output,
        dpi=150
    )

    plt.close()

    print("保存成功：")
    print(trip_output)

    # ============================================================
    # 7. 读取 ADS：区域收入 Top10
    # ============================================================

    print_title("读取 ADS 区域收入 Top10")

    revenue_path = os.path.join(
        ads_path,
        "ads_location_revenue_top10"
    )

    revenue_df = spark.read.parquet(
        revenue_path
    )

    revenue_df.show(
        10,
        truncate=False
    )

    # ============================================================
    # 8. 区域收入 Top10
    # ============================================================

    print_title("生成：区域收入 Top10")

    revenue_pd = (
        revenue_df
        .orderBy(
            col("total_revenue").desc()
        )
        .select(
            "Zone",
            "total_revenue"
        )
        .toPandas()
    )

    revenue_pd = revenue_pd.sort_values(
        "total_revenue"
    )

    plt.figure(figsize=(12, 7))

    plt.barh(
        revenue_pd["Zone"],
        revenue_pd["total_revenue"]
    )

    plt.title(
        "NYC Taxi - Top 10 Pickup Zones by Revenue"
    )

    plt.xlabel(
        "Total Revenue"
    )

    plt.ylabel(
        "Zone"
    )

    plt.tight_layout()

    revenue_output = os.path.join(
        visualization_path,
        "location_revenue_top10.png"
    )

    plt.savefig(
        revenue_output,
        dpi=150
    )

    plt.close()

    print("保存成功：")
    print(revenue_output)

    # ============================================================
    # 9. 读取 ADS：各 Borough Top3
    # ============================================================

    print_title("读取 ADS：各 Borough Top3")

    borough_path = os.path.join(
        ads_path,
        "ads_borough_location_top3"
    )

    borough_df = spark.read.parquet(
        borough_path
    )

    borough_df.show(
        30,
        truncate=False
    )

    # ============================================================
    # 10. 各 Borough Top3 订单量
    # ============================================================

    print_title("生成：各 Borough Top3")

    borough_pd = (
        borough_df
        .filter(
            col("Borough").isNotNull()
        )
        .select(
            "Borough",
            "Zone",
            "trip_count"
        )
        .orderBy(
            "Borough",
            col("trip_count").desc()
        )
        .toPandas()
    )

    plt.figure(figsize=(14, 8))

    # 每个 Borough 单独画一组
    for borough in borough_pd["Borough"].unique():

        temp = borough_pd[
            borough_pd["Borough"] == borough
        ]

        plt.bar(
            temp["Zone"],
            temp["trip_count"],
            label=borough
        )

    plt.title(
        "NYC Taxi - Top 3 Pickup Zones by Borough"
    )

    plt.xlabel(
        "Zone"
    )

    plt.ylabel(
        "Trip Count"
    )

    plt.xticks(
        rotation=60,
        ha="right"
    )

    plt.legend()

    plt.tight_layout()

    borough_output = os.path.join(
        visualization_path,
        "borough_location_top3.png"
    )

    plt.savefig(
        borough_output,
        dpi=150
    )

    plt.close()

    print("保存成功：")
    print(borough_output)

    # ============================================================
    # 11. 读取 ADS：整体运营指标
    # ============================================================

    print_title("读取 ADS：整体运营指标")

    overall_path = os.path.join(
        ads_path,
        "ads_overall_metrics"
    )

    overall_df = spark.read.parquet(
        overall_path
    )

    overall_df.show(
        truncate=False
    )

    # ============================================================
    # 12. 整体运营指标可视化
    # ============================================================

    print_title("生成：整体运营指标")

    overall_row = (
        overall_df
        .first()
    )

    total_trip_count = overall_row["total_trip_count"]
    total_revenue = overall_row["total_revenue"]
    avg_revenue = overall_row["avg_revenue"]
    avg_distance = overall_row["avg_distance"]

    print("总订单量：", total_trip_count)
    print("总收入：", total_revenue)
    print("整体客单价：", avg_revenue)
    print("平均里程：", avg_distance)

    metrics = {
        "Total Trips": total_trip_count,
        "Total Revenue": total_revenue,
        "Avg Revenue": avg_revenue,
        "Avg Distance": avg_distance
    }

    plt.figure(figsize=(10, 6))

    plt.axis("off")

    text = (
        "NYC Taxi Overall Metrics\n\n"
        f"Total Trips: {total_trip_count:,}\n\n"
        f"Total Revenue: ${total_revenue:,.2f}\n\n"
        f"Avg Revenue: ${avg_revenue:.2f}\n\n"
        f"Avg Distance: {avg_distance:.2f}"
    )

    plt.text(
        0.5,
        0.5,
        text,
        ha="center",
        va="center",
        fontsize=18
    )

    overall_output = os.path.join(
        visualization_path,
        "overall_metrics.png"
    )

    plt.savefig(
        overall_output,
        dpi=150,
        bbox_inches="tight"
    )

    plt.close()

    print("保存成功：")
    print(overall_output)

    # ============================================================
    # 13. 输出总结
    # ============================================================

    print_title("可视化任务完成")

    print("生成的文件：")

    print(
        "1.",
        hourly_output
    )

    print(
        "2.",
        trip_output
    )

    print(
        "3.",
        revenue_output
    )

    print(
        "4.",
        borough_output
    )

    print(
        "5.",
        overall_output
    )

    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()