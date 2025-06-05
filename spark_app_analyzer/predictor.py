import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def estimate_runtime_scaling(task_df, executor_wall_clock_sec, driver_wall_clock_sec, current_executors, critical_path_sec):
    if not current_executors or not executor_wall_clock_sec or not critical_path_sec:
        return pd.DataFrame([])  # early return if missing critical data

    critical_path_ms = critical_path_sec * 1000
    total_task_time_ms = task_df.agg({"executor_run_time_ms": "sum"}).first()[0]

    if not total_task_time_ms:
        return pd.DataFrame([])

    parallelizable_ms = total_task_time_ms - critical_path_ms
    total_wall_clock_sec = executor_wall_clock_sec + driver_wall_clock_sec

    # Avoid zero division for driver ratio
    driver_ratio = driver_wall_clock_sec / total_wall_clock_sec if total_wall_clock_sec else 0

    predictions = []
    for multiplier in [1.0, 2.0, 3.0, 4.0, 5.0]:
        new_executors = max(1, int(current_executors * multiplier))

        # Estimated executor runtime in seconds
        estimated_executor_sec = (critical_path_ms + (parallelizable_ms / new_executors)) / 1000.0

        denom = driver_wall_clock_sec + estimated_executor_sec
        overlap_weight = 1 - driver_wall_clock_sec / denom if denom else 0

        app_duration_sec = max(driver_wall_clock_sec, estimated_executor_sec) + \
                           overlap_weight * min(driver_wall_clock_sec, estimated_executor_sec)

        predictions.append({
            "Executor Count": new_executors,
            "Executor Multiplier": f"{int(multiplier * 100)}%",
            "Estimated Executor WallClock": f"{int(estimated_executor_sec // 60)}m {int(estimated_executor_sec % 60)}s",
            "Estimated Total Duration": f"{int(app_duration_sec // 60)}m {int(app_duration_sec % 60)}s",
        })

    return pd.DataFrame(predictions)