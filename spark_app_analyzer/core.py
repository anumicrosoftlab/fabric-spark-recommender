from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

def get_spark_session(app_name="SparkAppAnalyzer") -> SparkSession:
    """Initialize and return a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def extract_app_metadata(eventlog_df, artifact_type):
    """Extract application metadata such as app ID, app name, and key Spark config flags."""
    # Get appId and appName
    app_info_df = (
        eventlog_df
        .filter(col("App ID").isNotNull() & col("App Name").isNotNull())
        .select("App ID", "App Name")
        .distinct()
        .limit(1)
    )

    # Extract spark.native.enabled
    native_enabled_df = (
        eventlog_df
        .selectExpr("`Spark Properties`.`spark.native.enabled` AS spark_native_enabled")
        .filter(col("spark_native_enabled").isNotNull())
        .distinct()
        .limit(1)
    )

    # Extract fabric high concurrency setting
    hc_enabled_df = (
        eventlog_df
        .selectExpr("`Spark Properties`.`spark.trident.highconcurrency.enabled` AS fabric_hc_enabled")
        .filter(col("fabric_hc_enabled").isNotNull())
        .distinct()
        .limit(1)
    )

    spark_native_enabled = native_enabled_df.collect()[0]["spark_native_enabled"] if native_enabled_df.count() > 0 else None
    fabric_hc_enabled = hc_enabled_df.collect()[0]["fabric_hc_enabled"] if hc_enabled_df.count() > 0 else None


    # Enrich metadata DataFrame
    result_df = (
        app_info_df
        .withColumn("spark_native_enabled", lit(spark_native_enabled))
        .withColumn("fabric_hc_enabled", lit(fabric_hc_enabled))
        .withColumn("artifactType", lit(artifact_type))
    )

    return result_df