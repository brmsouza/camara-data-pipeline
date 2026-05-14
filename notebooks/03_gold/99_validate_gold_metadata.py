# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Metadata Governance Validation
# MAGIC
# MAGIC **Notebook:** 99_validate_gold_metadata
# MAGIC
# MAGIC Validates metadata governance coverage of Gold layer tables and columns.
# MAGIC
# MAGIC This notebook validates the semantic documentation of the Gold layer by detecting
# MAGIC schema drift, undocumented columns, missing metadata definitions and metadata
# MAGIC coverage gaps across analytical dimensions, fact tables and Gold views.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Discover physical Gold tables and columns
# MAGIC - Compare physical schemas against metadata definitions
# MAGIC - Detect tables documented but missing physically
# MAGIC - Detect columns documented but missing physically
# MAGIC - Detect physical columns missing from metadata definitions
# MAGIC - Calculate metadata coverage percentage
# MAGIC - Produce a validation report for governance review
# MAGIC - Support continuous metadata quality control
# MAGIC
# MAGIC **Source of truth:** `metadata_comments`  
# MAGIC
# MAGIC **Target:** Gold metadata validation report

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

SCHEMA_NAME = "gold"

# COMMAND ----------

gold_tables = [
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {SCHEMA_NAME}").collect()
]


# COMMAND ----------

physical_schema = {}

for table_name in gold_tables:

    full_table_name = f"{SCHEMA_NAME}.{table_name}"

    columns = [
        row.col_name
        for row in spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()
        if row.col_name and not row.col_name.startswith("#")
    ]

    physical_schema[full_table_name] = set(columns)

# COMMAND ----------

metadata_schema = {}

for item in metadata_comments:

    table_name = item["table"]

    metadata_schema[table_name] = set(
        item.get("columns", {}).keys()
    )


# COMMAND ----------

validation_results = []

# COMMAND ----------

for metadata_table in metadata_schema.keys():

    if metadata_table not in physical_schema:

        validation_results.append(
            Row(
                validation_type="TABLE_MISSING",
                table_name=metadata_table,
                column_name=None,
                status="ERROR",
                detail="Table exists in metadata but not physically in Gold layer."
            )
        )


# COMMAND ----------



for table_name, metadata_columns in metadata_schema.items():

    if table_name not in physical_schema:
        continue

    physical_columns = physical_schema[table_name]

    # -------------------------------------------------------------------------
    # Missing physical columns
    # -------------------------------------------------------------------------

    for metadata_column in metadata_columns:

        if metadata_column not in physical_columns:

            validation_results.append(
                Row(
                    validation_type="COLUMN_MISSING",
                    table_name=table_name,
                    column_name=metadata_column,
                    status="ERROR",
                    detail="Column exists in metadata but not physically in table."
                )
            )

    # -------------------------------------------------------------------------
    # Undocumented columns
    # -------------------------------------------------------------------------

    for physical_column in physical_columns:

        if physical_column not in metadata_columns:

            validation_results.append(
                Row(
                    validation_type="COLUMN_UNDOCUMENTED",
                    table_name=table_name,
                    column_name=physical_column,
                    status="WARNING",
                    detail="Physical column exists but is missing in metadata_comments."
                )
            )

# -----------------------------------------------------------------------------
# COVERAGE METRICS
# -----------------------------------------------------------------------------

total_physical_columns = sum(
    len(cols)
    for cols in physical_schema.values()
)

total_documented_columns = 0

for table_name, metadata_columns in metadata_schema.items():

    if table_name not in physical_schema:
        continue

    physical_columns = physical_schema[table_name]

    total_documented_columns += len(
        metadata_columns.intersection(physical_columns)
    )

coverage_percent = round(
    (
        total_documented_columns /
        total_physical_columns
    ) * 100,
    2
) if total_physical_columns > 0 else 0

# -----------------------------------------------------------------------------
# RESULTS DATAFRAME
# -----------------------------------------------------------------------------

if validation_results:

    df_validation = spark.createDataFrame(validation_results)

else:

    df_validation = spark.createDataFrame(
        [
            Row(
                validation_type="VALIDATION",
                table_name=None,
                column_name=None,
                status="SUCCESS",
                detail="No metadata inconsistencies detected."
            )
        ]
    )

# -----------------------------------------------------------------------------
# ADD EXECUTION METADATA
# -----------------------------------------------------------------------------

df_validation = (
    df_validation
    .withColumn(
        "validation_ts",
        current_timestamp()
    )
)


# COMMAND ----------

display(df_validation)

# COMMAND ----------

print("=" * 100)
print("GOLD METADATA GOVERNANCE VALIDATION")
print("=" * 100)

print(f"Gold tables discovered           : {len(physical_schema)}")
print(f"Physical columns discovered      : {total_physical_columns}")
print(f"Documented columns               : {total_documented_columns}")
print(f"Metadata coverage (%)            : {coverage_percent}%")

print("=" * 100)