from pyspark.sql import DataFrame, SparkSession, functions as F

INPUT_PATH = "/app/data/samples/sample_iot_logs.csv"
OUTPUT_PATH = "/app/data/processed/spark/device_features"
REQUIRED_COLUMNS = {
    "event_timestamp",
    "device_id",
    "protocol",
    "packet_size",
    "duration_ms",
    "event_type",
    "attack_type",
    "status",
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("iot-device-features-job")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )


def validate_required_columns(dataframe: DataFrame) -> None:
    missing_columns = sorted(REQUIRED_COLUMNS.difference(dataframe.columns))
    if missing_columns:
        raise ValueError(
            "Missing required columns in sample_iot_logs.csv: "
            + ", ".join(missing_columns)
        )


def normalize_input(dataframe: DataFrame) -> DataFrame:
    trimmed_attack_type = F.trim(F.coalesce(F.col("attack_type"), F.lit("")))

    return dataframe.select(
        F.to_timestamp(
            F.trim(F.col("event_timestamp")),
            "yyyy-MM-dd'T'HH:mm:ssX",
        ).alias("event_timestamp"),
        F.trim(F.col("device_id")).alias("device_id"),
        F.upper(F.trim(F.col("protocol"))).alias("protocol"),
        F.col("packet_size").cast("long").alias("packet_size"),
        F.col("duration_ms").cast("double").alias("duration_ms"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type"),
        F.when(trimmed_attack_type == "", F.lit(None))
        .otherwise(trimmed_attack_type)
        .alias("attack_type"),
        F.lower(F.trim(F.col("status"))).alias("status"),
    )


def compute_device_features(dataframe: DataFrame) -> DataFrame:
    is_attack = (F.col("event_type") == F.lit("attack")) | F.col("attack_type").isNotNull()
    is_failed = F.col("status").isin("failed", "error")
    is_success = F.col("status") == F.lit("success")

    features = (
        dataframe.groupBy("device_id")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("protocol").alias("unique_protocols"),
            F.sum("packet_size").alias("total_packet_size"),
            F.round(F.avg("packet_size"), 2).alias("avg_packet_size"),
            F.max("packet_size").alias("max_packet_size"),
            F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
            F.max("duration_ms").alias("max_duration_ms"),
            F.sum(F.when(is_failed, F.lit(1)).otherwise(F.lit(0))).alias("failed_events"),
            F.sum(F.when(is_success, F.lit(1)).otherwise(F.lit(0))).alias("success_events"),
            F.round(F.avg(F.when(is_failed, F.lit(1.0)).otherwise(F.lit(0.0))), 4).alias(
                "failed_event_ratio"
            ),
            F.sum(F.when(is_attack, F.lit(1)).otherwise(F.lit(0))).alias("attack_events"),
            F.round(F.avg(F.when(is_attack, F.lit(1.0)).otherwise(F.lit(0.0))), 4).alias(
                "attack_event_ratio"
            ),
            F.min("event_timestamp").alias("first_event_timestamp"),
            F.max("event_timestamp").alias("last_event_timestamp"),
        )
        .withColumn(
            "risk_level",
            F.when(
                (F.col("attack_event_ratio") >= F.lit(0.5))
                | (F.col("failed_event_ratio") >= F.lit(0.5)),
                F.lit("high"),
            )
            .when(
                (F.col("attack_event_ratio") > F.lit(0))
                | (F.col("failed_event_ratio") > F.lit(0)),
                F.lit("medium"),
            )
            .otherwise(F.lit("low")),
        )
        .orderBy("device_id")
    )

    return features.select(
        "device_id",
        "total_events",
        "unique_protocols",
        "total_packet_size",
        "avg_packet_size",
        "max_packet_size",
        "avg_duration_ms",
        "max_duration_ms",
        "failed_events",
        "success_events",
        "failed_event_ratio",
        "attack_events",
        "attack_event_ratio",
        "first_event_timestamp",
        "last_event_timestamp",
        "risk_level",
    )


def main() -> None:
    spark = build_spark_session()

    try:
        input_dataframe = (
            spark.read.option("header", "true")
            .option("mode", "FAILFAST")
            .csv(INPUT_PATH)
        )
        validate_required_columns(input_dataframe)

        input_row_count = input_dataframe.count()
        normalized_dataframe = normalize_input(input_dataframe)
        features_dataframe = compute_device_features(normalized_dataframe).cache()
        output_row_count = features_dataframe.count()

        features_dataframe.write.mode("overwrite").parquet(OUTPUT_PATH)

        print(f"Input row count: {input_row_count}")
        print(f"Output row count: {output_row_count}")
        print(f"Output path: {OUTPUT_PATH}")
        print("Device feature preview:")
        features_dataframe.show(10, truncate=False)
    finally:
        spark.stop()
        print("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()
