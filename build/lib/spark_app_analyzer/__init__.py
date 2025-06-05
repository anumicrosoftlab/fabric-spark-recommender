# spark_app_analyzer/__init__.py

from .core import get_spark_session, extract_app_metadata
from .metrics import compute_stage_task_summary, compute_application_runtime, compute_executor_wall_clock_time
from .predictor import estimate_runtime_scaling
from .recommendations import generate_recommendations

def analyze(eventlog_path: str, artifact_type: str):
    """
    Wrapper function that returns the 5 key DataFrames for one app_id:
    (app_metadata_df, stage_summary_df, metrics_df, predictions_df, recommendations_df)
    """
    spark = get_spark_session()
    eventlog_df = spark.read.json(eventlog_path)
    app_metadata_df = extract_app_metadata(eventlog_df)

    # Collect app IDs (assume there’s at least one)
    app_ids = [row["App ID"] for row in app_metadata_df.select("App ID").collect()]
    if not app_ids:
        raise ValueError("No App IDs found in event log metadata")

    # Just process the first app_id for simplicity — could be extended for multiple apps
    app_id = app_ids[0]

    stage_summary_df, metrics_df, predictions_df, recommendations_df = compute_stage_task_summary(eventlog_df, app_metadata_df, app_id)
    # Return all 5 (including app metadata)
    return app_metadata_df, stage_summary_df, metrics_df, predictions_df, recommendations_df