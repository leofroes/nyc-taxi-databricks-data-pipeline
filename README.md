# NYC Taxi Data Engineering Pipeline

End-to-end data engineering project processing **11.2M+ NYC Taxi trips** using **Apache Spark and Databricks**.

The project implements a distributed data pipeline combining **Batch Processing and Structured Streaming**, following a Medallion Architecture and using **Delta Lake** and **Unity Catalog** for data management and governance.

## Project Overview

The goal of this project is to design and implement a scalable data pipeline capable of processing large volumes of raw data and transforming them into structured, governed, and analytics-ready datasets.

The solution covers the complete data lifecycle:

**Data Sources → Bronze → Silver → Gold**

In addition to batch processing, the project includes a Structured Streaming pipeline with checkpointing, event-time processing, watermarking, and late-data handling.

## Tech Stack

- Apache Spark / PySpark
- Databricks
- Spark SQL
- Structured Streaming
- Delta Lake
- Unity Catalog
- Photon
- Adaptive Query Execution (AQE)

## Dataset

The project uses NYC Taxi trip data together with NYC Taxi Zone information.

### Sources

| Dataset | Format | Purpose |
|---|---|---|
| NYC Taxi Trips | Parquet | Main transactional dataset |
| NYC Taxi Zones | CSV | Geographic dimension used to enrich trip data |

The pipeline processes approximately:

**11,198,026 taxi trips**

This volume enables the exploration of distributed processing techniques using Apache Spark.

## Architecture

The batch pipeline follows a Medallion Architecture:

**Raw → Bronze → Silver → Gold**

### Bronze

Raw datasets are ingested and persisted as Delta tables with minimal transformation.

Main tables:

- `bronze.viagens`
- `bronze.zonas`

### Silver

The Silver layer is responsible for data cleaning, validation and enrichment.

Main transformations include:

- temporal feature engineering;
- null and duplicate analysis;
- trip duration calculation;
- data quality flags;
- data quality classification;
- geographic enrichment using joins;
- validation of row counts after joins.

The final Silver table contains **11,198,026 records**.

### Gold

The Gold layer contains analytics-ready datasets generated through:

- aggregations;
- business metrics;
- Window Functions;
- ranking;
- pivot operations;
- geographic analysis;
- trip-distance segmentation.

Examples of Gold tables include:

- `gold.viagens_mensal`
- `gold.viagens_zona_origem`
- `gold.viagens_zona_destino`
- `gold.ranking_mensal_distritos`
- `gold.viagens_pivot_distrito_mes`
- `gold.viagens_categoria_distancia`

## Structured Streaming

A second pipeline was implemented using **Spark Structured Streaming**.

Files are incrementally processed from a Unity Catalog Volume using:

- `readStream`
- Trigger `AvailableNow`
- Delta Lake sink
- Checkpointing

The streaming output is persisted into:

`bronze.viagens_streaming`

An additional event-time experiment was implemented using **window aggregations and watermarking** to evaluate late-data handling.

During the experiment, intentionally late events were introduced into the stream.

The Spark streaming metrics confirmed:

**17 late records dropped by the watermark.**

This demonstrates the practical behavior of stateful event-time processing rather than only configuring the watermark.

## Performance Optimization

Spark physical execution plans were analyzed using:

`explain("formatted")`

The analysis identified the use of:

- Catalyst Optimizer
- Adaptive Query Execution (AQE)
- Photon
- Broadcast Hash Join
- Predicate filtering
- Column projection
- Shuffle operations

### Join Strategy

The main trip dataset contains more than **11 million records**, while the geographic zone dimension is significantly smaller.

Spark automatically selected:

**PhotonBroadcastHashJoin**

for the geographic enrichment joins, avoiding unnecessary redistribution of the large trip dataset.

### Global Sort Experiment

A performance experiment evaluated the impact of an unnecessary global `orderBy`.

| | Original | Optimized |
|---|---:|---:|
| Physical plan stages | 14 | 10 |
| Execution time | 2.39 s | 1.17 s |
| Additional sort shuffle | Yes | No |
| PhotonSort | Yes | No |

The observed execution time decreased by approximately **51%**.

Execution times may vary in a Serverless environment, so the main optimization evidence is the structural simplification of the physical execution plan.

## Data Governance

The project uses **Unity Catalog** to organize and govern the data pipeline.

The catalog is structured into:

```text
nyc_taxi_data
│
├── raw
│   └── Volume: landing
│
├── bronze
│   ├── viagens
│   ├── viagens_streaming
│   └── zonas
│
├── silver
│   └── viagens
│
└── gold
    ├── viagens_mensal
    ├── viagens_zona_origem
    ├── viagens_zona_destino
    ├── ranking_mensal_distritos
    ├── viagens_pivot_distrito_mes
    └── viagens_categoria_distancia
```

The project also explores:

- managed Delta tables;
- catalog and schema organization;
- access privileges;
- Unity Catalog Volumes;
- end-to-end data lineage.

## Data Lineage

Unity Catalog lineage provides visibility into how data moves across the pipeline:

**Bronze → Silver → Gold**

The lineage graph makes it possible to trace the dependencies between the raw datasets, transformation layer, and analytical outputs.

## Key Results

| Metric | Result |
|---|---:|
| Trips processed | **11,198,026** |
| Source formats | **Parquet + CSV** |
| Architecture | **Raw → Bronze → Silver → Gold** |
| Processing | **Batch + Streaming** |
| Late events dropped by watermark | **17** |
| Optimization experiment | **2.39s → 1.17s** |
| Physical plan stages | **14 → 10** |
| Governance | **Unity Catalog** |
| Lineage | **Bronze → Silver → Gold** |

## Repository Structure

```text
nyc-taxi-databricks-data-pipeline/
│
├── notebooks/
│   ├── 01_Exploracao_Dados.ipynb
│   ├── 02_Ingestao_Camada_Bronze.ipynb
│   ├── 03_Transformacoes_Camada_Silver.ipynb
│   ├── 04_Camada_Gold.ipynb
│   ├── 05_Streaming.ipynb
│   ├── 06_Otimizacao.ipynb
│   └── 07_Governanca_Unity_Catalog.ipynb
│
├── README.md
├── .gitignore
└── LICENSE

## What I Learned

This project provided hands-on experience designing a distributed data pipeline beyond simply transforming a dataset.

The main engineering challenges explored were:

- designing data layers with clear responsibilities;
- processing millions of records with Spark;
- enriching large datasets efficiently;
- understanding physical execution plans;
- identifying unnecessary shuffle operations;
- implementing stateful event-time processing;
- handling late data with watermarking;
- implementing checkpoint-based streaming resilience;
- organizing and tracing data assets with Unity Catalog.

## Author

**Leonardo Fróes**

Data & Analytics professional focused on Data Engineering, Apache Spark, Databricks and cloud data platforms.
