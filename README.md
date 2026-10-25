# Medallion Batch Pipeline — Insurance Claims

End-to-end batch data pipeline in PySpark + Delta Lake implementing medallion
architecture: raw ingestion (Bronze), cleansing with quarantine (Silver), and a
star-schema serving layer (Gold), with incremental MERGE loads and a built-in
data-quality reconciliation report.

## Architecture

```
Landing (CSV extracts) -> Bronze (raw + ingestion metadata, append-only)
-> Silver (typed, deduplicated, standardized; invalid rows quarantined with reasons)
-> Gold (fact_claims + dim_policyholder, dim_policy, dim_claim_type, dim_date)
```

## Highlights

- Deduplication keeping the latest record per key (window functions)
- Quarantine pattern: bad rows isolated with a reason code, never silently dropped
- Incremental loads via Delta `MERGE INTO`, guarded by `updated_at` (no full rebuilds)
- Star schema with surrogate keys; smart date key (yyyyMMdd)
- DQ report reconciles every row: bronze = clean + quarantined + duplicates removed

## Verified results (two loads)

**Load 1:** 41 bronze claims -> 36 clean, 4 quarantined (missing amount, negative
amount, invalid date, orphan policy), 1 duplicate removed.

**Load 2 (delta):** 6 new rows -> 5 merged, 1 quarantined; 3 claim statuses updated
in place. Gold fact table: 41 rows; totals reconcile exactly across layers.

## Run it

```
pip install pyspark==3.5.1 delta-spark==3.2.0
python data_gen.py    # batch 1 source extracts
python bronze.py      # land raw
python silver.py      # clean + quarantine
python gold.py        # star schema + DQ report
python data_gen2.py   # batch 2 delta (new + updated claims)
python merge2.py      # incremental MERGE into Silver, then re-run gold.py
```

## Azure mapping

Local folders -> ADLS Gen2 containers | Scripts -> Databricks notebooks/jobs |
Step sequence -> Azure Data Factory pipeline | Gold layer -> Power BI semantic model
