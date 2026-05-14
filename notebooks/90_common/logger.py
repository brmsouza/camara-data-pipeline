# Databricks notebook source
# MAGIC %md
# MAGIC # Core Layer — Pipeline Logging Utilities
# MAGIC
# MAGIC **Notebook:** `logger`
# MAGIC
# MAGIC Provides standardized logging utilities for the data pipeline.
# MAGIC
# MAGIC This notebook centralizes logging behavior across notebooks and utility modules,
# MAGIC ensuring consistent log formatting, operational observability and integration
# MAGIC with pipeline monitoring structures.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Standardize operational logging across the data pipeline
# MAGIC - Support structured logging levels such as INFO, WARNING and ERROR
# MAGIC - Integrate logging events with `monitoring.pipeline_log`
# MAGIC - Support operational observability and troubleshooting workflows
# MAGIC - Provide reusable logging functions for ingestion and administrative notebooks
# MAGIC - Ensure consistent execution traceability across pipeline layers
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Supports structured logging such as INFO, ERROR and WARNING
# MAGIC - Integrates with `monitoring.pipeline_log` for observability
# MAGIC - Used across ingestion, transformation and admin notebooks
# MAGIC - Does not persist data directly, delegating persistence to `log_pipeline_event`

# COMMAND ----------

import logging


def get_logger(name: str, layer: str = "pipeline") -> logging.Logger:
    layer_norm = layer.strip().lower()
    logger_name = f"{layer_norm}.{name}"

    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(layer)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    class LayerFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.layer = layer_norm.upper()
            return True

    handler.addFilter(LayerFilter())
    logger.addHandler(handler)

    return logger