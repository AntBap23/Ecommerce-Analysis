# Ecommerce A/B Test Analysis

This project analyzes an ecommerce A/B test comparing the legacy landing page (`control` / `old_page`) against a new landing page (`treatment` / `new_page`). The workflow uses PostgreSQL for storage, Python for data access, and Jupyter for exploratory analysis.

## Project Goals

- Clean the raw experiment data and keep only valid control/treatment page combinations.
- Pull analysis-ready data from PostgreSQL into pandas.
- Measure conversion performance overall and by country.
- Check whether observed differences are statistically meaningful.
- Package the analysis for notebook exploration, GitHub presentation, and slide generation.

## Repository Structure

- [data/ab_data.csv](/Users/bapbap23/Desktop/Ecommerce%20Analysis/data/ab_data.csv): Raw A/B experiment data.
- [data/countries.csv](/Users/bapbap23/Desktop/Ecommerce%20Analysis/data/countries.csv): User-to-country lookup.
- [sql/fixing.sql](/Users/bapbap23/Desktop/Ecommerce%20Analysis/sql/fixing.sql): SQL used to validate and create `ab_data_clean`.
- [sql/analyze.sql](/Users/bapbap23/Desktop/Ecommerce%20Analysis/sql/analyze.sql): SQL analysis queries.
- [python/scripts/load_data.py](/Users/bapbap23/Desktop/Ecommerce%20Analysis/python/scripts/load_data.py): Load CSVs into PostgreSQL.
- [python/scripts/pull_from_sql.py](/Users/bapbap23/Desktop/Ecommerce%20Analysis/python/scripts/pull_from_sql.py): Pull `ab_data_clean` and `countries` from PostgreSQL into pandas.
- [notebooks/analysis_starter.ipynb](/Users/bapbap23/Desktop/Ecommerce%20Analysis/notebooks/analysis_starter.ipynb): Main notebook for analysis.
- [findings.txt](/Users/bapbap23/Desktop/Ecommerce%20Analysis/findings.txt): Slide-ready written findings for presentation drafting.

## Data Setup

Create a project `.env` file using [`.env.example`](/Users/bapbap23/Desktop/Ecommerce%20Analysis/.env.example):

```env
PG_HOST=localhost
PG_PORT=5432
PG_DB=your_database_name
PG_USER=your_postgres_user
PG_PASSWORD=
LOAD_MODE=replace
PG_SCHEMA=public
```

Load the raw CSVs into Postgres if needed:

```bash
source .venv/bin/activate
python python/scripts/load_data.py
```

Create the cleaned table in Postgres:

```sql
-- Run the statements in sql/fixing.sql
CREATE TABLE ab_data_clean AS
SELECT *
FROM ab_data
WHERE
    ("group" = 'control' AND landing_page = 'old_page')
 OR ("group" = 'treatment' AND landing_page = 'new_page');
```

## Running The Notebook

Install dependencies and start Jupyter:

```bash
source .venv/bin/activate
pip install -r requirements.txt
jupyter lab
```

Open [notebooks/analysis_starter.ipynb](/Users/bapbap23/Desktop/Ecommerce%20Analysis/notebooks/analysis_starter.ipynb) and run the cells. The notebook pulls directly from PostgreSQL via [python/scripts/pull_from_sql.py](/Users/bapbap23/Desktop/Ecommerce%20Analysis/python/scripts/pull_from_sql.py), so you do not need a separate export step first.

## Analysis Summary

The final cleaned experiment includes roughly 290.6k records split almost evenly between control and treatment. Conversion rates are very close:

- Control conversion rate: 12.04%
- Treatment conversion rate: 11.88%
- Estimated lift: -0.16 percentage points
- Two-proportion z-test p-value: 0.19

The main conclusion is that the new landing page does not outperform the old landing page. Country-level cuts for the US, UK, and Canada show only minor differences, and those differences do not materially change the result.

## Key Findings

- The treatment page underperforms the control page slightly, but the gap is not statistically significant.
- Geographic differences are small and do not explain away the null result.
- Session duration is broadly similar across groups, suggesting no strong engagement lift from the treatment experience.
- A tiny number of duplicate `user_id` records exist, but a user-level sensitivity check leads to the same conclusion.

## Presentation Support

Use [findings.txt](/Users/bapbap23/Desktop/Ecommerce%20Analysis/findings.txt) as the source document for generating a PowerPoint or executive summary in another tool such as Claude.
