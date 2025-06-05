import logging
from pyspark.sql import Row, DataFrame as SparkDataFrame
import pandas as pd
from spark_app_analyzer import core, metrics

# Setup logging
logging.basicConfig(level=logging.INFO)

def main():
    spark = core.get_spark_session("SparkAppAnalyzer")
    
    # Load event log
    eventlog_path = "abfss://workspaceCnf@onelake.dfs.fabric.microsoft.com/sparklens_api.Lakehouse/Files/application_1748610049218_0001_1"
    artifact_type="SynapseNotebook"
    eventlog_df = spark.read.json(eventlog_path)

    # Extract metadata
    metadata_df = core.extract_app_metadata(eventlog_df, artifact_type)
    app_ids = metadata_df.select("App ID").rdd.flatMap(lambda x: x).collect()
    logging.info(f"Found {len(app_ids)} application(s): {app_ids}")

    summary_dfs = []
    metrics_dfs = []
    predictions_dfs = []
    recommendations_dfs = []

    for i, app_id in enumerate(app_ids):
        logging.info(f"Processing application {i+1}/{len(app_ids)}: {app_id}")

        start_event = (
            eventlog_df
            .filter(eventlog_df["Event"] == "SparkListenerApplicationStart")
            .select("Timestamp")
            .limit(1)
            .collect()
        )

        if not start_event:
            logging.warning(f"Missing SparkListenerApplicationStart for {app_id}")
            error_row = Row(applicationID=app_id, error="Missing SparkListenerApplicationStart event")
            summary_dfs.append(spark.createDataFrame([error_row]))
            continue

        try:
            summary_df, metrics_df, prediction_df, recommendation_df = metrics.compute_stage_task_summary(
                eventlog_df, metadata_df, app_id
            )
            summary_dfs.append(summary_df)
            metrics_dfs.append(metrics_df)
            predictions_dfs.append(prediction_df)
            recommendations_dfs.append(recommendation_df)
        except Exception as e:
            logging.error(f"Error processing app {app_id}: {e}")
            error_row = Row(applicationID=app_id, error=str(e))
            summary_dfs.append(spark.createDataFrame([error_row]))

    # Combine dataframes
    def combine_dfs(dfs):
        if not dfs:
            return None
        base = dfs[0]
        for df in dfs[1:]:
            base = base.unionByName(df, allowMissingColumns=True)
        return base

    summary_combined = combine_dfs(summary_dfs)
    metrics_combined = combine_dfs([spark.createDataFrame(df) if isinstance(df, pd.DataFrame) else df for df in metrics_dfs])
    predictions_combined = combine_dfs([spark.createDataFrame(df) if isinstance(df, pd.DataFrame) else df for df in predictions_dfs])
    recommendations_combined = combine_dfs([spark.createDataFrame(df) if isinstance(df, pd.DataFrame) else df for df in recommendations_dfs])

    # Output or save results
    logging.info("Summary:")
    summary_combined.show(truncate=False)

    logging.info("Metrics:")
    metrics_combined.show(truncate=False)

    logging.info("Predictions:")
    predictions_combined.show(truncate=False)

    logging.info("Recommendations:")
    recommendations_combined.show(truncate=False)


if __name__ == "__main__":
    main()