# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Curated Layer — Legislative Organization Membership Consolidation and Enrichment
# MAGIC
# MAGIC **Notebook:** `09_curated_orgaos_membros`
# MAGIC
# MAGIC Consolidates, enriches and validates legislative organization
# MAGIC membership data from the Silver Base layer.
# MAGIC
# MAGIC This notebook transforms `silver_base.orgaos_membros` into a curated and
# MAGIC analytics-ready dataset representing the relationship between deputies and
# MAGIC legislative organizations such as committees, plenary sessions and governing
# MAGIC bodies.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Consolidate standardized organization membership attributes from Silver Base
# MAGIC - Curate membership role and status indicators
# MAGIC - Create analytical membership flags
# MAGIC - Preserve deputy, party, UF and organization relationships
# MAGIC - Preserve membership temporal attributes and technical validation flags
# MAGIC - Preserve complete lineage and traceability columns
# MAGIC - Validate curated-level uniqueness
# MAGIC - Persist curated Delta tables for Gold consumption
# MAGIC
# MAGIC **Source of truth:** `silver_base.orgaos_membros`  
# MAGIC
# MAGIC **Target:** `silver_curated.orgaos_membros`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake format
# MAGIC - Source for committee participation and organizational analytics

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    upper,
    current_timestamp,
    when,
    lit,
    current_date,
)

# COMMAND ----------

SOURCE_TABLE = "silver_base.orgaos_membros"
TARGET_TABLE = "silver_curated.orgaos_membros"

PIPELINE_NAME = "silver_curated_orgaos_membros"
LAYER = "silver_curated"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
records_discarded = 0

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={PIPELINE_NAME} | start successfully",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

df_base = spark.table(SOURCE_TABLE)

records_read = df_base.count()


# COMMAND ----------

df_curated = (
    df_base
    .select(
        # ---------------------------------------------------
        # Organization relationship
        # ---------------------------------------------------

        col("org_id_orgao")
            .alias("org_id_orgao"),

        col("org_tx_uri")
            .alias("org_tx_uri"),

        col("org_sg_orgao")
            .alias("org_sg_orgao"),

        col("org_tx_nome")
            .alias("org_tx_nome"),

        col("org_tx_nome_publicacao")
            .alias("org_tx_nome_publicacao"),

        # ---------------------------------------------------
        # Deputy relationship
        # ---------------------------------------------------

        col("dept_id_deputado")
            .alias("dept_id_deputado"),

        col("dept_tx_uri")
            .alias("dept_tx_uri"),

        col("dept_tx_nome")
            .alias("dept_tx_nome"),

        col("part_sg_partido")
            .alias("part_sg_partido"),

        col("uf_sg_uf")
            .alias("uf_sg_uf"),

        # ---------------------------------------------------
        # Membership information
        # ---------------------------------------------------

        col("memb_tx_cargo")
            .alias("memb_tx_cargo"),

        col("memb_dt_inicio")
            .alias("memb_dt_inicio"),

        col("memb_fl_data_inicio_valida")
            .alias("memb_fl_data_inicio_valida"),

        col("memb_dt_fim")
            .alias("memb_dt_fim"),

        col("memb_fl_data_fim_valida")
            .alias("memb_fl_data_fim_valida"),

        col("memb_fl_periodo_valido")
            .alias("memb_fl_periodo_valido"),

        when(col("memb_dt_fim").isNull(), lit(1))
            .otherwise(lit(0))
            .alias("memb_fl_ativo"),

        when(
            col("memb_dt_fim").isNull(),
            lit("Ativo")
        )
        .when(
            col("memb_dt_fim") >= current_date(),
            lit("Ativo")
        )
        .otherwise(lit("Encerrado"))
        .alias("memb_tx_status"),

        when(
            upper(col("memb_tx_cargo")).contains("PRESIDENT"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_presidente"),

        when(
            upper(col("memb_tx_cargo")).contains("RELATOR"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_relator"),

        when(
            upper(col("memb_tx_cargo")).contains("VICE"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_vice"),

        # ---------------------------------------------------
        # Technical key
        # ---------------------------------------------------

        col("memb_tx_dedup_key")
            .alias("memb_tx_dedup_key"),

        # ---------------------------------------------------
        # Bronze lineage / traceability
        # ---------------------------------------------------

        col("bronze_ts_ingestao")
            .alias("bronze_ts_ingestao"),

        col("bronze_dt_ingestao")
            .alias("bronze_dt_ingestao"),

        col("bronze_tx_endpoint")
            .alias("bronze_tx_endpoint"),

        col("bronze_id_origem")
            .alias("bronze_id_origem"),

        col("bronze_tx_source_file")
            .alias("bronze_tx_source_file"),

        col("bronze_id_batch")
            .alias("bronze_id_batch"),

        col("bronze_tx_record_hash")
            .alias("bronze_tx_record_hash"),

        # ---------------------------------------------------
        # Silver Base lineage
        # ---------------------------------------------------

        col("silver_ts_processamento")
            .alias("silver_base_ts_processamento"),

        # ---------------------------------------------------
        # Silver Curated metadata
        # ---------------------------------------------------

        current_timestamp()
            .alias("silver_curated_ts_processamento")
    )
)

# COMMAND ----------

duplicated_members = (
    df_curated
    .groupBy("memb_tx_dedup_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_members > 0:
    raise Exception(
        f"Data quality error: {duplicated_members} duplicated organization members in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("memb_tx_dedup_key").isNull()
        |
        col("org_id_orgao").isNull()
        |
        col("dept_id_deputado").isNull()
    )
    .withColumn(
        "rejection_reason",
        when(
            col("memb_tx_dedup_key").isNull(),
            lit("memb_tx_dedup_key_is_null")
        )
        .when(
            col("org_id_orgao").isNull(),
            lit("org_id_orgao_is_null")
        )
        .when(
            col("dept_id_deputado").isNull(),
            lit("dept_id_deputado_is_null")
        )
        .otherwise(lit("unknown"))
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("memb_tx_dedup_key").isNotNull())
    .filter(col("org_id_orgao").isNotNull())
    .filter(col("dept_id_deputado").isNotNull())
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_valid.count()

records_discarded = df_discarded.count()

# COMMAND ----------

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("memb_tx_dedup_key").isNull()
        |
        col("org_id_orgao").isNull()
        |
        col("dept_id_deputado").isNull()
    )
    .withColumn(
        "rejection_reason",
        when(
            col("memb_tx_dedup_key").isNull(),
            lit("memb_tx_dedup_key_is_null")
        )
        .when(
            col("org_id_orgao").isNull(),
            lit("org_id_orgao_is_null")
        )
        .when(
            col("dept_id_deputado").isNull(),
            lit("dept_id_deputado_is_null")
        )
        .otherwise(lit("unknown"))
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("memb_tx_dedup_key").isNotNull())
    .filter(col("org_id_orgao").isNotNull())
    .filter(col("dept_id_deputado").isNotNull())
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_valid.count()

records_discarded = df_discarded.count()

# COMMAND ----------

(
    df_discarded.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET_TABLE}_rejeitadas")
)

# COMMAND ----------

(
    df_discarded.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET_TABLE}_rejeitadas")
)

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("memb_tx_dedup_key").isNotNull())
    .filter(col("org_id_orgao").isNotNull())
    .filter(col("dept_id_deputado").isNotNull())
)

records_written = df_valid.count()

records_discarded = records_read - records_written

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_finished",
    message=f"source={PIPELINE_NAME} | finished successfully | records_read={records_read} | records_written={records_written} | records_discarded={records_discarded}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
    finished_at=datetime.now(),
)

# COMMAND ----------

print(f"Pipeline: {PIPELINE_NAME}")
print(f"Layer: {LAYER}")
print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")
print(f"Records discarded: {records_discarded}")