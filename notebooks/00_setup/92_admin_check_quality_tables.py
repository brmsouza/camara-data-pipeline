# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Layer — Table Row Count and Quality Validation
# MAGIC
# MAGIC **Notebook:** `92_admin_check_table_row_counts`
# MAGIC
# MAGIC Performs row count and basic quality checks across pipeline tables.
# MAGIC
# MAGIC This notebook is used to validate data availability, identify empty tables and
# MAGIC detect basic inconsistencies after ingestion and transformation pipeline
# MAGIC executions.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Perform row count validation across pipeline tables
# MAGIC - Detect empty or partially loaded tables
# MAGIC - Support operational quality verification after pipeline execution
# MAGIC - Assist troubleshooting and reprocessing analysis
# MAGIC - Validate data availability across analytical layers
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Executes dynamically across selected schemas
# MAGIC - Does not modify data
# MAGIC - Supports operational validation and troubleshooting

# COMMAND ----------

from pyspark.sql.functions import col, max as spark_max

schemas_to_check = ["bronze", "monitoring", "config"]

results = []

for schema in schemas_to_check:
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()

    for table in tables:
        table_name = table.tableName
        full_name = f"{schema}.{table_name}"

        try:
            df = spark.table(full_name)
            columns = df.columns

            total_rows = df.count()

            null_source_id = (
                df.filter(col("source_id").isNull()).count()
                if "source_id" in columns
                else None
            )

            distinct_source_id = (
                df.select("source_id").distinct().count()
                if "source_id" in columns
                else None
            )

            distinct_record_hash = (
                df.select("record_hash").distinct().count()
                if "record_hash" in columns
                else None
            )

            duplicate_hash_count = (
                total_rows - distinct_record_hash
                if distinct_record_hash is not None
                else None
            )

            latest_ingestion_timestamp = (
                df.select(
                    spark_max("ingestion_timestamp").alias("latest_ingestion_timestamp")
                ).collect()[0]["latest_ingestion_timestamp"]
                if "ingestion_timestamp" in columns
                else None
            )

            results.append({
                "schema": schema,
                "table": table_name,
                "total_rows": total_rows,
                "null_source_id": null_source_id,
                "distinct_source_id": distinct_source_id,
                "duplicate_hash_count": duplicate_hash_count,
                "latest_ingestion_timestamp": latest_ingestion_timestamp,
                "status": "OK",
                "error_message": None,
            })

            print(f"{full_name} -> {total_rows}")

        except Exception as e:
            results.append({
                "schema": schema,
                "table": table_name,
                "total_rows": None,
                "null_source_id": None,
                "distinct_source_id": None,
                "duplicate_hash_count": None,
                "latest_ingestion_timestamp": None,
                "status": "ERROR",
                "error_message": str(e),
            })

            print(f"ERROR reading {full_name}: {e}")

df_quality = spark.createDataFrame(results)

display(df_quality.orderBy("schema", "table"))

display(
    df_quality
    .filter(
        (col("status") == "ERROR") |
        (col("total_rows") == 0) |
        (col("null_source_id") > 0) |
        (col("duplicate_hash_count") > 0)
    )
    .orderBy("schema", "table")
)

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, max as spark_max

schemas_to_check = ["bronze", "monitoring"]

results = []

for schema in schemas_to_check:
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()

    for table in tables:
        table_name = table.tableName
        full_name = f"{schema}.{table_name}"

        try:
            df = spark.table(full_name)

            total_rows = df.count()

            # Só tenta validar colunas técnicas se existirem
            columns = df.columns

            null_source_id = df.filter(col("source_id").isNull()).count() if "source_id" in columns else None
            distinct_source_id = df.select("source_id").distinct().count() if "source_id" in columns else None

            distinct_record_hash = df.select("record_hash").distinct().count() if "record_hash" in columns else None
            duplicate_hash_count = (total_rows - distinct_record_hash) if distinct_record_hash is not None else None

            latest_ingestion = (
                df.select(spark_max("ingestion_timestamp").alias("latest")).collect()[0]["latest"]
                if "ingestion_timestamp" in columns
                else None
            )

            results.append({
                "schema": schema,
                "table": table_name,
                "total_rows": total_rows,
                "null_source_id": null_source_id,
                "distinct_source_id": distinct_source_id,
                "duplicate_hash_count": duplicate_hash_count,
                "latest_ingestion_timestamp": latest_ingestion,
            })
 

        except Exception as e:
            results.append({
                "schema": schema,
                "table": table_name,
                "total_rows": None,
                "null_source_id": None,
                "distinct_source_id": None,
                "duplicate_hash_count": None,
                "latest_ingestion_timestamp": None,
                "error": str(e),
            })

            print(f"ERROR reading {full_name}: {e}")

df_validation = spark.createDataFrame(results)

display(df_validation.orderBy("schema", "table"))

# COMMAND ----------

display(
    df_validation.filter(
        (col("total_rows").isNull()) |
        (col("total_rows") == 0) |
        (col("null_source_id") > 0) |
        (col("duplicate_hash_count") > 0)
    )
)

# COMMAND ----------

for row in df_validation.collect():
    full_name = f"{row['schema']}.{row['table']}"

    try:
        df = spark.table(full_name)

        if "source_system" in df.columns:
            print(f"\n{full_name} - volume by source")

            display(
                df.groupBy("source_system", "source_endpoint")
                .count()
                .orderBy("source_system")
            )

    except:
        pass