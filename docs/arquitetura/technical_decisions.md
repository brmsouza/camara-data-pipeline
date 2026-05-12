## Partitioning Strategy

The pipeline uses different partitioning strategies across the Medallion layers.

Bronze tables are partitioned by ingestion date to support replay, auditing, and operational recovery.

Silver and Gold tables are partitioned by business columns such as year, month, legislature, and state, depending on the data domain and query patterns.

High-cardinality columns such as deputy ID, supplier document number, and supplier name are not used as partition columns to avoid small files and excessive metadata overhead.


## Development Strategy

This project adopts a hybrid development approach combining local development using VS Code and distributed data processing using Databricks.

### Rationale

The decision to split development between VS Code and Databricks is based on software engineering best practices, scalability requirements, and maintainability considerations.

---

## VS Code Responsibilities

VS Code is used as the primary development environment for:

- Project structure organization
- Development of reusable Python modules (`src/`)
- Configuration management (`.yml` files)
- Documentation (README, architecture, runbooks)
- Version control (Git)

### Benefits

- Improved code organization and modularity
- Better readability and maintainability
- Strong integration with Git and version control workflows
- Easier unit testing and code validation
- Separation of concerns between logic and execution

---

## Databricks Responsibilities

Databricks is used as the execution and processing environment for:

- Running Spark workloads
- Executing data ingestion pipelines
- Processing large-scale datasets
- Writing and managing Delta tables (Bronze, Silver, Gold)
- Performing data exploration and validation

### Benefits

- Distributed processing with Apache Spark
- Native integration with Delta Lake
- Scalable execution for large datasets
- Built-in tools for monitoring and optimization
- Interactive notebooks for data inspection

---

## Integration Strategy

The integration between VS Code and Databricks follows a Git-based workflow:

1. Code is developed and versioned locally using VS Code
2. Changes are committed and pushed to GitHub
3. Databricks pulls the latest version using Git folders
4. Notebooks in Databricks import and execute reusable modules from `src/`

---

## Architectural Alignment

This approach aligns with the Medallion Architecture:

- **Bronze Layer**: Ingestion logic defined in VS Code, executed in Databricks
- **Silver Layer**: Transformation logic modularized and reused across pipelines
- **Gold Layer**: Analytical models built using structured and reusable components

---

## Why Not Use Only Databricks?

While Databricks notebooks can handle the entire pipeline, relying exclusively on notebooks introduces several limitations:

- Reduced code modularity
- Limited reusability
- Harder version control and code review
- Increased notebook complexity
- Lower maintainability in production scenarios

---

## Conclusion

The hybrid approach ensures a clear separation between development and execution, resulting in a more scalable, maintainable, and production-ready data pipeline.
