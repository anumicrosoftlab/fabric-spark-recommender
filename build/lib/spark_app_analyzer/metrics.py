from pyspark.sql import Row
from pyspark.sql.functions import lit 
from pyspark.sql.functions import (
    col, count, countDistinct, avg, expr,
    percentile_approx, min as spark_min, max as spark_max, sum as spark_sum
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd
from .predictor import estimate_runtime_scaling
from .recommendations import generate_recommendations


def compute_application_runtime(event_log_df):
    start_row = event_log_df.filter(col("Event") == "SparkListenerApplicationStart") \
        .select((col("Timestamp") / 1000).cast("timestamp").alias("start_time")) \
        .limit(1).collect()

    end_row = event_log_df.filter(col("Event") == "SparkListenerApplicationEnd") \
        .select((col("Timestamp") / 1000).cast("timestamp").alias("end_time")) \
        .limit(1).collect()

    if start_row and end_row:
        return (end_row[0]["end_time"].timestamp() - start_row[0]["start_time"].timestamp())
    return 0.0


def compute_executor_wall_clock_time(event_log_df):
    task_end_df = event_log_df.filter(col("Event") == "SparkListenerTaskEnd") \
        .select(
            col("Task Info.Launch Time").alias("start_time"),
            col("Task Info.Finish Time").alias("end_time")
        ).dropna()

    intervals = task_end_df.selectExpr("start_time / 1000 as start_sec", "end_time / 1000 as end_sec") \
        .orderBy("start_sec")

    merged_intervals = []
    for row in intervals.collect():
        start, end = row["start_sec"], row["end_sec"]
        if not merged_intervals or merged_intervals[-1][1] < start:
            merged_intervals.append([start, end])
        else:
            merged_intervals[-1][1] = max(merged_intervals[-1][1], end)

    return sum(end - start for start, end in merged_intervals)


def compute_stage_task_summary(event_log_df, metadata_df, app_id):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    task_end_df = event_log_df.filter(col("Event") == "SparkListenerTaskEnd").withColumn("applicationID", col(lit(app_id)))

    tasks_df = task_end_df.select(
        col("Stage ID").alias("stage_id"),
        col("Stage Attempt ID").alias("stage_attempt_id"),
        col("Task Info.Task ID").alias("task_id"),
        col("Task Info.Executor ID").alias("executor_id"),
        col("Task Info.Launch Time").alias("launch_time"),
        col("Task Info.Finish Time").alias("finish_time"),
        col("Task Info.Failed").alias("failed"),
        (col("Task Metrics.Executor Run Time") / 1000).alias("duration_sec"),
        (col("Task Metrics.Input Metrics.Bytes Read") / 1024 / 1024).alias("input_mb"),
        col("Task Metrics.Input Metrics.Records Read").alias("input_records"),
        (col("Task Metrics.Shuffle Read Metrics.Remote Bytes Read") / 1024 / 1024).alias("shuffle_read_mb"),
        col("Task Metrics.Shuffle Read Metrics.Total Records Read").alias("shuffle_read_records"),
        (col("Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written") / 1024 / 1024).alias("shuffle_write_mb"),
        col("Task Metrics.Shuffle Write Metrics.Shuffle Records Written").alias("shuffle_write_records"),
        (col("Task Metrics.Output Metrics.Bytes Written") / 1024 / 1024).alias("output_mb"),
        col("Task Metrics.Output Metrics.Records Written").alias("output_records")
    ).filter(col("failed") == False)

    stage_duration_df = tasks_df.groupBy("stage_id", "stage_attempt_id").agg(
        spark_min("launch_time").alias("min_launch_time"),
        spark_max("finish_time").alias("max_finish_time"),
        countDistinct("executor_id").alias("num_executors")
    ).withColumn(
        "stage_execution_time_sec", expr("(max_finish_time - min_launch_time) / 1000")
    )

    stage_summary_df = tasks_df.groupBy("stage_id", "stage_attempt_id").agg(
        count("task_id").alias("num_tasks"),
        spark_min("duration_sec").alias("min_duration_sec"),
        spark_max("duration_sec").alias("max_duration_sec"),
        avg("duration_sec").alias("avg_duration_sec"),
        percentile_approx("duration_sec", 0.75).alias("p75_duration_sec"),
        avg("shuffle_read_mb").alias("avg_shuffle_read_mb"),
        avg("shuffle_write_mb").alias("avg_shuffle_write_mb"),
        avg("input_mb").alias("avg_input_mb"),
        avg("output_mb").alias("avg_output_mb")
    )

    final_summary_df = stage_summary_df.join(
        stage_duration_df, on=["stage_id", "stage_attempt_id"], how="left"
    ).orderBy(col("stage_execution_time_sec").desc()).limit(5)

    app_duration_sec = compute_application_runtime(event_log_df)
    executor_wall_clock_sec = compute_executor_wall_clock_time(event_log_df)
    driver_wall_clock_sec = app_duration_sec - executor_wall_clock_sec
    max_executors = tasks_df.select("executor_id").distinct().count()

    per_stage_max_df = tasks_df.groupBy("stage_id").agg(spark_max("duration_sec").alias("max_task_time_sec"))
    critical_path_row = per_stage_max_df.agg(spark_sum("max_task_time_sec").alias("critical_path_time_sec")).first()
    critical_path_sec = critical_path_row["critical_path_time_sec"]

    task_df = tasks_df.withColumn("executor_run_time_ms", col("duration_sec") * 1000)

    schema = StructType([
        StructField("Executor Count", IntegerType(), True),
        StructField("Executor Multiplier", StringType(), True),
        StructField("Estimated Executor WallClock", StringType(), True),
        StructField("Estimated Total Duration", StringType(), True)
    ])
    empty_df = spark.createDataFrame([], schema)

    if critical_path_sec:
        predictions_df = estimate_runtime_scaling(task_df, executor_wall_clock_sec, driver_wall_clock_sec, max_executors, critical_path_sec)
    else:
        predictions_df = empty_df

    metrics = [
        ("Application Duration (sec)", round(app_duration_sec, 2)),
        ("Executor Wall Clock Time (sec)", round(executor_wall_clock_sec, 2)),
        ("Driver Wall Clock Time (sec)", round(driver_wall_clock_sec, 2)),
        ("Executor Time % of App Time", round(100 * executor_wall_clock_sec / app_duration_sec, 2)),
        ("Driver Time % of App Time", round(100 * driver_wall_clock_sec / app_duration_sec, 2)),
        ("Max Executors", max_executors),
        ("Critical Path Time (sec)", round(critical_path_sec, 2))
    ]

    metrics_rows = [Row(app_id=app_id, metric=key, value=float(value)) for key, value in metrics]

    metrics_schema = StructType([
        StructField("app_id", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("value", DoubleType(), False)
    ])
    metrics_df = spark.createDataFrame(metrics_rows, schema=metrics_schema)

    recommendations_df = generate_recommendations(app_duration_sec, driver_wall_clock_sec, executor_wall_clock_sec, metadata_df, task_df)

    return final_summary_df, metrics_df, predictions_df, recommendations_df