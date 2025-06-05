# spark_app_analyzer_and_recommender

`spark_app_analyzer` is a Python package for analyzing Spark application event logs to extract performance metrics, runtime estimations, and optimization recommendations. It helps Spark users identify bottlenecks and improve job efficiency by providing detailed summaries and actionable insights.

---

## Features

- Extract Spark application metadata from event logs
- Compute detailed stage and task summaries
- Estimate runtime scaling with different executor counts
- Generate recommendations to optimize Spark job performance
- Works with Spark event logs stored on OneLake

---

## Installation

~~~ bash
pip install spark_app_analyzer-0.1.0-py3-none-any.whl
~~~

Or upload to environment and publish.

## Usage

from spark_app_analyzer import analyze

eventlog_path = "onelake path"
artifact_type = "SynapseNotebook"

app_metadata_df, stage_summary_df, metrics_df, predictions_df, recommendations_df = analyze(eventlog_path, artifact_type)

app_metadata_df.show()
stage_summary_df.show()
metrics_df.show()
predictions_df.show()
recommendations_df.show()


