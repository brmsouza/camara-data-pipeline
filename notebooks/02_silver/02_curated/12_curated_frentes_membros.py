# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 12_curated_frentes_membros
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, enriches and validates parliamentary front
# membership data from Silver Base.
#
# Context:
# This notebook transforms silver_base.frentes_membros into a curated and
# analytics-ready dataset representing the relationship between deputies and
# parliamentary fronts. The resulting table supports downstream analysis of
# thematic participation, political grouping, parliamentary coalitions and
# correlation with votes and expenses.
#
# Responsibilities:
# - Consolidate standardized parliamentary front membership attributes from Silver Base
# - Curate membership role and status indicators
# - Create analytical membership flags
# - Preserve deputy, party, UF, legislature and front relationships
# - Preserve membership temporal attributes and technical validation flags
# - Preserve complete lineage and traceability columns
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Source:
# silver_base.frentes_membros
#
# Target:
# silver_curated.frentes_membros
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Source for parliamentary front participation and coalition analytics
# ------------------------------------------------------------------------------

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

SOURCE_TABLE = "silver_base.frentes_membros"
TARGET_TABLE = "silver_curated.frentes_membros"

PIPELINE_NAME = "silver_curated_frentes_membros"
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
        # Parliamentary front relationship
        # ---------------------------------------------------

        col("frente_id_frente")
            .alias("frente_id_frente"),

        # ---------------------------------------------------
        # Deputy relationship
        # ---------------------------------------------------

        col("dept_id_deputado")
            .alias("dept_id_deputado"),

        col("dept_tx_uri")
            .alias("dept_tx_uri"),

        col("dept_tx_nome")
            .alias("dept_tx_nome_parlamentar"),

        col("dept_tx_email")
            .alias("dept_tx_email"),

        col("dept_fl_email_valido")
            .alias("dept_fl_email_valido"),

        col("dept_tx_url_foto")
            .alias("dept_tx_url_foto"),

        # ---------------------------------------------------
        # Party / UF / legislature
        # ---------------------------------------------------

        col("part_sg_partido")
            .alias("part_sg_partido"),

        col("part_tx_uri")
            .alias("part_tx_uri"),

        col("uf_sg_uf")
            .alias("uf_sg_uf"),

        col("leg_id_legislatura")
            .alias("leg_id_legislatura"),

        # ---------------------------------------------------
        # Membership information
        # ---------------------------------------------------

        col("memb_cd_titulo")
            .alias("memb_cd_titulo"),

        col("memb_tx_titulo")
            .alias("memb_tx_titulo"),

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

        when(
            col("memb_dt_fim").isNull(),
            lit(1)
        )
        .when(
            col("memb_dt_fim") >= current_date(),
            lit(1)
        )
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
            upper(col("memb_tx_titulo")).contains("COORDENADOR"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_coordenador"),

        when(
            upper(col("memb_tx_titulo")).contains("PRESIDENTE"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_presidente"),

        when(
            upper(col("memb_tx_titulo")).contains("VICE"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_vice"),

        when(
            upper(col("memb_tx_titulo")).contains("MEMBRO"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_membro"),

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
        f"Data quality error: {duplicated_members} duplicated front members in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("memb_tx_dedup_key").isNotNull())
    .filter(col("frente_id_frente").isNotNull())
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