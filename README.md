# API_dashboard

## DuckDB Upsert Utility

The helper `_duckdb_upsert_df` removes duplicate rows based on the provided key columns before inserting into DuckDB. If any duplicates are dropped, the function reports how many rows were removed. Pass `strict=True` to raise an exception if duplicates are detected instead of automatically dropping them.

