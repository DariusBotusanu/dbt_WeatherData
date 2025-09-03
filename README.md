# dbt_WeatherData (demo project)

A small project that experiments with **dbt (data build tool)** and **Google BigQuery** to transform weather data into analysis-ready tables, which are then used to train a simple machine learning model.

---

## Why This Project?

Modern data workflows rely heavily on **transforming raw data into clean, reliable, and structured datasets**.  
- **dbt** makes this process easy by bringing **software engineering best practices** (modularity, version control, testing, documentation) into the analytics engineering world.  
- **BigQuery** serves as the data warehouse where dbt runs its transformations.  
- To make things more practical, this project extends beyond data modeling: the transformed data feeds into a small **ML model** that predicts weather-related values.

This repository is meant as a **sandbox** to explore these ideas—not as a production-ready project.

---

## What is dbt?

[dbt](https://docs.getdbt.com/) is a framework for **transforming data in your warehouse using SQL and Jinja templating**.  

Some key concepts:

- **Models**: Each `.sql` file in dbt is a “model” that represents a transformation (usually a `SELECT` query). dbt materializes these models as **tables** or **views** in your warehouse.
- **DAG (Directed Acyclic Graph)**: dbt understands model dependencies and automatically builds a graph. For example, if `marts.daily_weather` depends on `staging.raw_weather`, dbt ensures staging runs first.
- **Testing**: You can define tests (e.g., “no nulls in this column”, “values are unique”) to validate data quality.
- **Documentation**: dbt automatically generates docs and lineage diagrams.

The big advantage: **instead of manually writing SQL scripts and running them in order, dbt orchestrates everything cleanly and transparently.**

---

## Project Workflow

This repo follows a **layered transformation approach**:

### 1. Raw Data
Weather data is ingested from an API (`api_data_extraction.py`) and loaded into BigQuery in raw format.  
At this stage the data may be:
- inconsistent,
- have strange field names,
- or contain missing values.

### 2. Staging Models
The **staging layer** acts as a cleaning and normalization step.  
- Standardizes naming conventions (e.g., `temp` → `temperature_celsius`).  
- Applies basic data type conversions (strings to numbers, timestamps to proper formats).  
- Removes or flags bad/missing values.

👉 **Think of staging as “make the raw data usable.”**

After the models are defined, dbt can be simply run from the terminal using the following command: dbt run

## Prerequisites

Before running this project, make sure you have the following installed:

- **Python 3.9+** (for running scripts and dbt)
- **dbt-core** and **dbt-bigquery** (`pip install dbt-bigquery`)
- **Google Cloud CLI (gcloud)**  
  - Required for authentication and project setup.
- **A Google Cloud Project** with **BigQuery enabled**.

### Authentication

This project uses **OAuth-based authentication via the Google Cloud CLI**.  

1. Install the [Google Cloud CLI](https://cloud.google.com/sdk/docs/install).  
2. Log in to your Google account:
   ```bash
   gcloud auth application-default login


