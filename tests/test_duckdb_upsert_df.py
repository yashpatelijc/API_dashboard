import ast
import duckdb
import pandas as pd
import pathlib
import pytest


def _load_upsert_function():
    path = pathlib.Path(__file__).resolve().parents[1] / "qh_api_dashboard.py"
    source = path.read_text()
    module = ast.parse(source)
    func_code = None
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_duckdb_upsert_df":
            func_code = ast.get_source_segment(source, node)
            break
    if func_code is None:
        raise AssertionError("_duckdb_upsert_df not found")
    namespace = {"pd": pd}
    exec(func_code, namespace)
    return namespace["_duckdb_upsert_df"]


def test_duckdb_upsert_df_missing_key():
    upsert = _load_upsert_function()
    con = duckdb.connect()
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    with pytest.raises(ValueError, match="Missing key columns: c"):
        upsert(con, "test", df, ["a", "c"])
    con.close()
