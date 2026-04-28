## Partitioning Strategy

The pipeline uses different partitioning strategies across the Medallion layers.

Bronze tables are partitioned by ingestion date to support replay, auditing, and operational recovery.

Silver and Gold tables are partitioned by business columns such as year, month, legislature, and state, depending on the data domain and query patterns.

High-cardinality columns such as deputy ID, supplier document number, and supplier name are not used as partition columns to avoid small files and excessive metadata overhead.
