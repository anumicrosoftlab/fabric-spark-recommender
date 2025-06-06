# 📊 Spark Application Insights Notebook

This PySpark notebook processes Spark event logs from Azure Data Explorer (Kusto), analyzes application behavior, and populates `eventHouseTelemetry` tables with summaries, performance metrics, predictions, and recommendations.

---

## 🔧 Configuration

### 1. Kusto Cluster Settings

Update the following placeholders in the Notebook:

~~~ python
kustoUri = "<https://your-kusto-cluster.kusto.data.microsoft.com>"
~~~

## 📈 Output

The notebook generates and processes data into **5 outputs per application**:

- ✅ `metadata_df`: metadata of the run
- ✅ `summary_df`: Top 5 slowest Spark stages  
- 📊 `metrics_df`: Execution and performance metrics  
- 🔮 `predictions_df`: Estimated durations with varied executor counts  
- 💡 `recommendations_df`: Actionable performance suggestions  

You can optionally write these to Kusto or other storage (see **[Optional: Output to Kusto]** section below).

---

## ⏰ Scheduling

To run this notebook daily, configure it using pipeline. 
