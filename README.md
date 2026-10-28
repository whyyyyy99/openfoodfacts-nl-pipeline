# Open Food Facts pipeline

A batch pipeline that ingests the daily [Open Food Facts](https://world.openfoodfacts.org/) product deltas into PostgreSQL and keeps a current-state table of every product.

## Background

Open Food Facts is a crowdsourced database of food products. Besides a full export, it publishes a delta file each day with the products changed since the previous one. Those deltas are only kept for about 14 days, so a pipeline that stops for longer silently loses data.

This project loads those deltas reliably (retries, integrity checks, idempotent reruns), keeps every version it has seen, and maintains the latest version of each product for analysis.

## Business questions

The goal is to compare the nutritional quality of food sold across European countries:

- How do Nutri-Score grades and average sugar and salt content differ between countries?
- Do countries that adopted Nutri-Score differ from those that did not?
- How do supermarket private labels compare with brand manufacturers in each country?
- Which products and brands were reformulated, for example to cut sugar or salt?
- How complete is the data for each country?

The current version builds the ingestion and current-state layers. The country marts that answer these questions are next on the roadmap.

## Architecture

```
static.openfoodfacts.org/data/delta
        │  extract: download, retry, gzip integrity check
        ▼
data/*.json.gz
        │  load: one transaction per file, skipped if already loaded
        ▼
raw.product_records        append-only, original JSON payload
        │  transform: parse and validate fields
        ▼
staging.product_records    typed columns, validation errors and quality warnings
        │  refresh: newest valid version per product code
        ▼
mart.products              one row per product
```

- **raw** keeps every record exactly as received, so staging and mart can be rebuilt after a parsing change.
- **staging** records why a row is invalid instead of dropping it.
- **mart** is refreshed incrementally: only product codes touched by new rows are recomputed from their full history, so a late-arriving older file cannot overwrite a newer version.
- A daily Airflow DAG ([dags/openfoodfacts_daily.py](dags/openfoodfacts_daily.py)) runs extract, a source-gap check, load and transform. Sunday runs do a full mart refresh as a weekly consistency check.

## Running it

Requires Python 3.12 and Docker.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d --wait        # PostgreSQL on localhost:5433

python -m src.schema               # create schemas and tables
python -m src.pipeline --count 2   # download and process the two latest deltas
```

Other entry points:

- `python -m src.pipeline <file names>` processes specific delta files.
- `python -m src.pipeline --rebuild` rebuilds staging and mart from raw.
- `python -m src.transform --full-refresh` recomputes every product in the mart from staging.

`DATABASE_URL` overrides the default connection and `OFF_DATA_DIR` sets where delta files are stored (default `data/`).

## Tests

```bash
pytest
```

Tests never use the network. Integration tests run against a disposable `<database>_test` database when PostgreSQL is reachable, and are skipped otherwise (`RUN_DB_TESTS=0` skips them explicitly). The DAG tests are skipped when Airflow is not installed.

[tests/fixtures/](tests/fixtures/) holds 70 unmodified records from two consecutive delta files. They include products present in both files, a record with an empty product code, products without a Nutri-Score and products sold outside Europe.

## Data and license

Product data comes from [Open Food Facts](https://world.openfoodfacts.org/) and is available under the [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/). The records in `tests/fixtures/` are covered by that license.

The code is released under the [MIT License](LICENSE).
