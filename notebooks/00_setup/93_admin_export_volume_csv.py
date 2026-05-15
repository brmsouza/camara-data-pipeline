# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Utilities Layer — Gold Analytical CSV Export
# MAGIC
# MAGIC **Notebook:** `93_admin_export_csv_vol`
# MAGIC
# MAGIC Exports Gold analytical views into CSV files stored in Unity Catalog Volumes.
# MAGIC
# MAGIC This notebook extracts analytical datasets from the Gold layer and persists
# MAGIC them as CSV files inside Unity Catalog Volumes. The exported datasets are
# MAGIC intended for external analysis, academic evaluation, BI demonstrations,
# MAGIC offline validation and analytical evidence generation.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read Gold analytical views
# MAGIC - Export analytical datasets as CSV files
# MAGIC - Persist CSV outputs into Unity Catalog Volumes
# MAGIC - Standardize analytical export naming conventions
# MAGIC - Support external analytical validation and demonstrations
# MAGIC - Support challenge evidence extraction and presentation
# MAGIC - Register operational export metrics
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC - Gold analytical views
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC - Unity Catalog Volume CSV exports
# MAGIC
# MAGIC ## Export Path
# MAGIC
# MAGIC - `/Volumes/camara/gold/data/csv`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - CSV exports generated with headers
# MAGIC - One output directory generated per analytical view
# MAGIC - Intended for analytical extraction and external consumption

# COMMAND ----------

views = [
    "gold.vw_alinhamento_frente_vs_partido",
    "gold.vw_analise_ineficiencia_parlamentar",
    "gold.vw_anomalias_ceap_zscore",
    "gold.vw_atividade_parlamentar_analitica",
    "gold.vw_ausencias_votacoes_criticas",
    "gold.vw_dashboard_partidos",
    "gold.vw_densidade_eventos_semanal",
    "gold.vw_deputados_mais_frentes",
    "gold.vw_despesas_ceap_analitica",
    "gold.vw_despesas_deputado_segmento",
    "gold.vw_especializacao_tematica",
    "gold.vw_eventos_analitica",
    "gold.vw_eventos_futuros",
    "gold.vw_evolucao_frentes_legislatura",
    "gold.vw_fidelidade_partidaria",
    "gold.vw_frentes_diversidade_partidaria",
    "gold.vw_frentes_membros_analitica",
    "gold.vw_gastos_segmentados",
    "gold.vw_indice_eficiencia_parlamentar",
    "gold.vw_indice_transparencia",
    "gold.vw_orientacoes_bancada_analitica",
    "gold.vw_partidos_analitica",
    "gold.vw_partidos_despesas_segmento",
    "gold.vw_partidos_especializacao_tematica",
    "gold.vw_partidos_fidelidade_votacao",
    "gold.vw_partidos_perfil",
    "gold.vw_partidos_votos_distribuicao",
    "gold.vw_perfil_gasto_partido",
    "gold.vw_perfil_parlamentar",
    "gold.vw_ranking_ausencias_criticas",
    "gold.vw_ranking_despesas_deputado_mensal",
    "gold.vw_score_engajamento_parlamentar",
    "gold.vw_semanas_sem_atividade",
    "gold.vw_sobreposicao_frentes",
    "gold.vw_top_10_gastos_partido_mensal",
    "gold.vw_votacoes_analitica",
    "gold.vw_votos_deputados_analitica"
]

base_path = "/Volumes/camara/gold/data/csv"

for view_name in views:
    output_name = view_name.replace(".", "_")
    output_dir = f"{base_path}/{output_name}"
    final_csv_path = f"{output_dir}/{output_name}.csv"

    print(f"Exporting all data from {view_name}")

    (
        spark.sql(f"SELECT * FROM {view_name}")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_dir)
    )

    files = dbutils.fs.ls(output_dir)
    csv_files = [f.path for f in files if f.path.endswith(".csv")]

    if not csv_files:
        raise Exception(f"No CSV file generated for {view_name}")

    dbutils.fs.mv(csv_files[0], final_csv_path)

    print(f"Created: {final_csv_path}")

print("All Gold analytical views exported successfully.")

# COMMAND ----------

views = [
    "gold_cdc.vw_proposicoes_tramitacao_historica",
    "gold_cdc.vw_tempo_tramitacao_proposicoes",
    "gold_cdc.vw_alertas_tramitacao_proposicoes",
    "monitoring.vw_sla_votacoes_streaming"
]

base_path = "/Volumes/camara/gold/data/csv"

for view_name in views:
    output_name = view_name.replace(".", "_")
    output_dir = f"{base_path}/{output_name}"
    final_csv_path = f"{output_dir}/{output_name}.csv"

    print(f"Exporting all data from {view_name}")

    (
        spark.sql(f"SELECT * FROM {view_name}")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_dir)
    )

    files = dbutils.fs.ls(output_dir)
    csv_files = [f.path for f in files if f.path.endswith(".csv")]

    if not csv_files:
        raise Exception(f"No CSV file generated for {view_name}")

    dbutils.fs.mv(csv_files[0], final_csv_path)

    print(f"Created: {final_csv_path}")

print("All Gold analytical views exported successfully.")

# COMMAND ----------

# ------------------------------------------------------------------------------
# Replace large exports with GitHub-safe sample exports
# ------------------------------------------------------------------------------

views = [
    "gold.vw_despesas_ceap_analitica",
    "gold.vw_votos_deputados_analitica"
]

base_path = "/Volumes/camara/gold/data/csv"

sample_limit = 10000

for view_name in views:

    output_name = view_name.replace(".", "_")

    output_dir = f"{base_path}/{output_name}_sample"

    final_csv_path = (
        f"{output_dir}/{output_name}_sample.csv"
    )

    print(f"Creating SAMPLE export for {view_name}")

    # ------------------------------------------------------------------------------
    # Generate sample dataset
    # ------------------------------------------------------------------------------

    (
        spark.sql(f"""
            SELECT *
            FROM {view_name}
            LIMIT {sample_limit}
        """)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_dir)
    )

    files = dbutils.fs.ls(output_dir)

    csv_files = [
        f.path for f in files
        if f.path.endswith(".csv")
    ]

    if not csv_files:
        raise Exception(f"No CSV generated for {view_name}")

    dbutils.fs.mv(
        csv_files[0],
        final_csv_path,
        True
    )

    print(f"Sample export created: {final_csv_path}")

print("Sample analytical exports created successfully.")