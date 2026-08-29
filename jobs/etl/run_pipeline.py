#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL 流水线调度脚本
====================

按"依赖顺序"精确调度 jobs/etl/ 目录下的所有 ETL 脚本。

特性：
    1. 依赖顺序固定，不会串步
    2. 每步执行后校验产物（输出目录存在且非空）
    3. 任一步失败立即停止（fail-fast），可加 --continue-on-error 继续
    4. 支持只跑指定步骤 / 跳过指定步骤
    5. 全程日志记录（控制台 + 文件）
    6. 幂等：增量脚本可重复运行，重复跑不会产生重复数据

用法示例：
    python run_pipeline.py                       # 跑全部核心步骤
    python run_pipeline.py --skip 02,16          # 跳过 DWD 和 MySQL 两步
    python run_pipeline.py --steps 02,03         # 只跑 02、03 两步
    python run_pipeline.py --continue-on-error   # 某步失败后继续跑
    python run_pipeline.py --check               # 只校验已有产物，不运行
"""

import os
import sys
import time
import shutil
import argparse
import datetime
import subprocess

# ============================================================
# 路径
# ============================================================

ETL_DIR = os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = os.path.abspath(
    os.path.join(ETL_DIR, "../..")
)

LOG_DIR = os.path.join(PROJECT_ROOT, "data", "logs")


# ============================================================
# 步骤定义（顺序即依赖顺序，不可随意调整）
#
# 字段说明：
#   id        步骤编号，用于 --steps / --skip
#   name      步骤名称
#   script    脚本文件名（相对 jobs/etl/）
#   outputs   产物路径（相对项目根目录），用于运行后校验
#   optional  是否可选步骤（默认不跑，除非显式用 --steps 指定）
#   mysql     是否依赖 MySQL（16 前会做连通性预检）
# ============================================================

STEPS = [
    {
        "id": "02",
        "name": "DWD 明细层（增量）",
        "script": "02_taxi_dwd.py",
        "outputs": ["data/warehouse/dwd/dwd_taxi_trip"],
        "optional": False,
    },
    {
        "id": "03",
        "name": "DWS 每日指标（增量）",
        "script": "03_dws_daily.py",
        "outputs": ["data/warehouse/dws/dws_daily_taxi"],
        "optional": False,
    },
    {
        "id": "04",
        "name": "DWS 小时指标",
        "script": "04_dws_hourly.py",
        "outputs": ["data/warehouse/dws/dws_hourly_taxi"],
        "optional": False,
    },
    {
        "id": "05",
        "name": "DWS 区域指标",
        "script": "05_dws_location.py",
        "outputs": ["data/warehouse/dws/dws_location_taxi"],
        "optional": False,
    },
    {
        "id": "06",
        "name": "Zone 维表预览（仅展示）",
        "script": "06_dimension_zone.py",
        "outputs": [],
        "optional": True,
    },
    {
        "id": "07",
        "name": "DWS 区域 + 维表",
        "script": "07_dws_location_zone.py",
        "outputs": ["data/warehouse/dws/dws_location_zone"],
        "optional": False,
    },
    {
        "id": "08",
        "name": "ADS 区域订单量 Top10",
        "script": "08_analysis_location.py",
        "outputs": ["data/warehouse/ads/location_trip_top10"],
        "optional": False,
    },
    {
        "id": "09",
        "name": "ADS 区域收入 Top10",
        "script": "09_ads_location_revenue_top10.py",
        "outputs": ["data/warehouse/ads/ads_location_revenue_top10"],
        "optional": False,
    },
    {
        "id": "10",
        "name": "ADS 区域平均客单价 Top10",
        "script": "10_ads_location_avg_revenue_top10.py",
        "outputs": ["data/warehouse/ads/ads_location_avg_revenue_top10"],
        "optional": False,
    },
    {
        "id": "11",
        "name": "ADS 各 Borough Top3",
        "script": "11_ads_borough_location_top3.py",
        "outputs": ["data/warehouse/ads/ads_borough_location_top3"],
        "optional": False,
    },
    {
        "id": "12",
        "name": "ADS 区域数据质量",
        "script": "12_ads_location_data_quality.py",
        "outputs": ["data/warehouse/ads/ads_location_data_quality"],
        "optional": False,
    },
    {
        "id": "13",
        "name": "ADS 整体运营指标",
        "script": "13_ads_overall_metrics.py",
        "outputs": ["data/warehouse/ads/ads_overall_metrics"],
        "optional": False,
    },
    {
        "id": "14",
        "name": "DWS 一致性检查（仅检查）",
        "script": "14_check_dws_location.py",
        "outputs": [],
        "optional": True,
    },
    {
        "id": "15",
        "name": "可视化出图",
        "script": "15_visualization.py",
        "outputs": ["data/visualization"],
        "optional": True,
    },
    {
        "id": "16",
        "name": "加载 MySQL（upsert）",
        "script": "16_load_mysql.py",
        "outputs": [],
        "optional": False,
        "mysql": True,
    },
]


# ============================================================
# 辅助函数
# ============================================================

def now_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def output_dir_valid(path):
    """校验产物目录：存在且包含至少一个 parquet 文件（非空）。"""
    full = os.path.join(PROJECT_ROOT, path)
    if not os.path.isdir(full):
        return False
    for root, _, files in os.walk(full):
        if any(f.endswith(".parquet") for f in files):
            return True
    return False


def mysql_reachable():
    """预检 MySQL 是否可连接（只用于 16 步骤前的提醒）。"""
    try:
        import mysql.connector
        connection = mysql.connector.connect(
            host="127.0.0.1",
            port=3307,
            user="root",
            password="FLzx3qcYSyhL9t",
            database="nyc_taxi",
            connect_timeout=3,
        )
        connection.close()
        return True
    except Exception:
        return False


# ============================================================
# 主流程
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="NYC Taxi ETL 流水线调度脚本"
    )
    parser.add_argument(
        "--steps",
        help="只运行指定的步骤编号，逗号分隔，如：02,03,16",
        default=None,
    )
    parser.add_argument(
        "--skip",
        help="跳过指定的步骤编号，逗号分隔，如：02,16",
        default=None,
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="某一步失败后继续执行后续步骤（默认失败即停）",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="只校验各步骤产物是否存在，不实际运行",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="不写日志文件",
    )
    return parser.parse_args()


def resolve_steps(args):
    """根据 --steps / --skip 筛选出要运行的步骤列表。"""
    if args.steps:
        wanted = {s.strip() for s in args.steps.split(",") if s.strip()}
        result = [s for s in STEPS if s["id"] in wanted]
    else:
        result = [s for s in STEPS if not s["optional"]]

    if args.skip:
        skipped = {s.strip() for s in args.skip.split(",") if s.strip()}
        result = [s for s in result if s["id"] not in skipped]

    return result


def run_step(step, log_file, check_only):
    """运行单个步骤，返回 (成功, 耗时秒)。"""
    id_ = step["id"]
    name = step["name"]
    script = step["script"]
    script_path = os.path.join(ETL_DIR, script)

    # 脚本文件必须存在
    if not os.path.isfile(script_path):
        emit(log_file, f"[{id_}] ✗ 脚本不存在：{script_path}")
        return False, 0.0

    if check_only:
        ok = all(output_dir_valid(p) for p in step["outputs"]) \
            if step["outputs"] else None
        if ok is None:
            status = "（无产物，跳过校验）"
        elif ok:
            status = "✓ 产物存在"
        else:
            status = "✗ 产物缺失"
        emit(log_file, f"[{id_}] {name} {status}")
        return (ok is not False), 0.0

    # MySQL 预检提醒
    if step.get("mysql") and not mysql_reachable():
        emit(log_file, f"[{id_}] ⚠ MySQL 连接失败，请确认 MySQL 已启动！")

    emit(log_file, f"[{id_}] ▶ 开始：{name}（{script}）")

    start = time.time()

    process = subprocess.Popen(
        [sys.executable, script_path],
        cwd=ETL_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # 实时读取输出，同时打印到控制台和日志文件
    for line in process.stdout:
        emit(log_file, line.rstrip())

    process.wait()
    elapsed = time.time() - start

    if process.returncode != 0:
        emit(log_file, f"[{id_}] ✗ 失败：{name}（退出码 {process.returncode}）")
        return False, elapsed

    # 产物校验
    if step["outputs"]:
        for out in step["outputs"]:
            if not output_dir_valid(out):
                emit(log_file, f"[{id_}] ✗ 产物缺失：{out}")
                return False, elapsed

    emit(log_file, f"[{id_}] ✓ 完成：{name}（耗时 {elapsed:.1f}s）")
    return True, elapsed


def emit(log_file, line):
    """同时输出到控制台和日志文件。"""
    text = f"{now_str()} | {line}"
    print(text, flush=True)
    if log_file is not None:
        log_file.write(text + "\n")
        log_file.flush()


def main():

    args = parse_args()

    # 筛选步骤
    steps = resolve_steps(args)

    if not steps:
        print("没有可运行的步骤，请检查 --steps / --skip 参数。")
        sys.exit(1)

    # 日志文件
    log_file = None
    if not args.no_log:
        os.makedirs(LOG_DIR, exist_ok=True)
        log_name = (
            f"run_pipeline_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        log_path = os.path.join(LOG_DIR, log_name)
        log_file = open(log_path, "w", encoding="utf-8")
        emit(log_file, "=" * 80)
        emit(log_file, "NYC Taxi ETL 流水线开始")
        emit(log_file, f"日志文件：{log_path}")
        emit(log_file, "=" * 80)

    emit(log_file, f"计划运行 {len(steps)} 个步骤：")
    for s in steps:
        tag = "（可选）" if s["optional"] else ""
        emit(log_file, f"  {s['id']}. {s['name']} {tag}")

    # 幂等性提醒
    emit(log_file, "提示：增量脚本（02/03）幂等，重复运行不会产生重复数据。")

    # ---- 执行 ----
    total_start = time.time()
    ok_count = 0
    failed = []

    for step in steps:

        if args.check:
            ok, _ = run_step(step, log_file, check_only=True)
            if not ok and step["outputs"]:
                failed.append(step)
            else:
                ok_count += 1
            continue

        ok, _ = run_step(step, log_file, check_only=False)

        if ok:
            ok_count += 1
        else:
            failed.append(step)
            if not args.continue_on_error:
                emit(
                    log_file,
                    f"✗ 步骤 {step['id']} 失败，流水线停止。"
                    f"（加 --continue-on-error 可继续）",
                )
                break

    total_elapsed = time.time() - total_start

    # ---- 汇总 ----
    emit(log_file, "=" * 80)
    emit(log_file, "流水线执行汇总")
    emit(log_file, "=" * 80)
    emit(log_file, f"成功：{ok_count} / {len(steps)}")
    emit(log_file, f"总耗时：{total_elapsed:.1f}s")

    if failed:
        emit(
            log_file,
            "失败步骤：" + ", ".join(f"({s['id']}) {s['name']}" for s in failed),
        )

    if log_file is not None:
        log_file.close()

    # 退出码：有失败则非 0
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断，流水线停止。")
        sys.exit(130)
