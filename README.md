# Retail Ecommerce – Azure Medallion Architecture

Proyecto de Data Engineering desarrollado en Azure utilizando arquitectura Medallion.

## Tecnologías

- Azure Data Factory
- Azure Databricks
- Azure Data Lake Storage Gen2
- Power BI
- PySpark

## Arquitectura

Databricks → Bronze → Silver → Gold → Power BI

## Capas

### Bronze
Datos raw generados desde Databricks.

### Silver
Procesamiento, calidad, deduplicación y gobierno de datos.

### Gold
KPIs y tablas analíticas para consumo de negocio.

## Tablas Gold

- fact_ventas
- fact_rfm_clientes
- fact_inventario
- dim_productos

## Funcionalidades

- Deduplicación
- Conditional Split
- SHA256 masking
- Manejo de errores
- Pipeline maestro
- Segmentación RFM
- Alertas inventario

## Dashboard

Dashboard ejecutivo desarrollado en Power BI conectado a la capa Gold.

## Autor

Javier Rodriguez Masmela