from pyspark.sql import SparkSession, functions as F


def main() -> None:
    spark = (
        SparkSession.builder.appName("iot-spark-smoke-job")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        input_rows = [
            {"device_id": "device-001", "protocol": "MQTT", "bytes_sent": 120},
            {"device_id": "device-002", "protocol": "HTTP", "bytes_sent": 80},
            {"device_id": "device-003", "protocol": "MQTT", "bytes_sent": 200},
            {"device_id": "device-004", "protocol": "CoAP", "bytes_sent": 40},
        ]

        dataframe = spark.createDataFrame(input_rows)

        aggregated = (
            dataframe.groupBy("protocol")
            .agg(
                F.count("*").alias("event_count"),
                F.sum("bytes_sent").alias("total_bytes_sent"),
            )
            .orderBy("protocol")
        )

        print("Stage 9A Spark smoke job aggregation result:")
        aggregated.show(truncate=False)
        print(
            "Aggregated rows:",
            [row.asDict() for row in aggregated.collect()],
        )
    finally:
        spark.stop()
        print("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()
