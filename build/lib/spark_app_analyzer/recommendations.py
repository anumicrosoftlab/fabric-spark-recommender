from pyspark.sql import Row
from pyspark.sql import SparkSession


def generate_recommendations(app_duration_sec, driver_wall_clock_sec, executor_wall_clock_sec, metadata_df, task_df):
    spark = SparkSession.builder.getOrCreate()
    recs = []

    if not app_duration_sec or app_duration_sec == 0:
        return spark.createDataFrame([Row(app_id=None, recommendation="App duration missing, unable to generate recommendations.")])

    driver_pct = 100 * driver_wall_clock_sec / app_duration_sec
    executor_pct = 100 * executor_wall_clock_sec / app_duration_sec

    # Extract metadata values safely
    app_id = None
    spark_native_enabled = None
    fabric_hc_enabled = None
    artifact_type = None

    if "App ID" in metadata_df.columns:
        app_id_row = metadata_df.select("App ID").first()
        app_id = app_id_row[0] if app_id_row else None

    if "spark_native_enabled" in metadata_df.columns:
        row = metadata_df.select("spark_native_enabled").first()
        if row:
            spark_native_enabled = str(row[0]).lower()

    if "fabric_hc_enabled" in metadata_df.columns:
        row = metadata_df.select("fabric_hc_enabled").first()
        if row:
            fabric_hc_enabled = str(row[0]).lower()

    if "artifactType" in metadata_df.columns:
        row = metadata_df.select("artifactType").first()
        if row:
            artifact_type = row[0]

    if driver_pct > 70:
        recs.append("This Spark job is driver-heavy (driver time > 70%). Consider parallelizing more operations to offload work to executors.")

    if spark_native_enabled in ["false", "0", "no", "off"] and executor_pct > 50:
        recs.append("Native Execution Engine (NEE) is disabled, but executors are doing significant work. Enable NEE for performance gains without added cost.")

    if driver_pct > 99:
        recs.append("This appears to be Python-native code running entirely on the driver. Run on Fabric Python kernel or refactor into Spark code for better parallelism.")

    if fabric_hc_enabled in ["false", "0", "no", "off"] and artifact_type == "SynapseNotebook":
        recs.append("High Concurrency is disabled for Fabric Notebook. Consider enabling High Concurrency mode to pack more notebooks into fewer sessions and save costs.")

    if not recs:
        recs = ["No performance recommendations found."]

    rec_rows = [Row(app_id=app_id, recommendation=rec) for rec in recs]

    return spark.createDataFrame(rec_rows)