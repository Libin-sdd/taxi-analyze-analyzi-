from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = (
        SparkSession.builder
        .appName("TaxiZoneDimension")
        .master("local[1]")
        .getOrCreate()
    )

    zone_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("../../data/warehouse/dimension/NYC_Taxi_Zones_20260828.csv")
    )

    print("=" * 80)
    print("Taxi Zone 维表")
    print("=" * 80)

    zone_df.printSchema()
    zone_df = zone_df.dropDuplicates(["Location ID"])

    zone_df = zone_df.select(
    col("Location ID").alias("LocationID"),
    col("Zone"),
    col("Borough")
)

    zone_df.show(10, truncate=False)

    print("维表数据量：", zone_df.count())

    spark.stop()


if __name__ == "__main__":
    main()