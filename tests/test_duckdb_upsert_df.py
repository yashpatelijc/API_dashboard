import ast
import pathlib
import pandas as pd
import pytest


def load_duckdb_upsert():
    path = pathlib.Path(__file__).resolve().parents[1] / "qh_api_dashboard.py"
    source = path.read_text()
    module_ast = ast.parse(source)
    func_node = next(n for n in module_ast.body if isinstance(n, ast.FunctionDef) and n.name == "_duckdb_upsert_df")
    module = ast.Module(body=[func_node], type_ignores=[])
    code = compile(module, filename=str(path), mode="exec")
    globals_dict = {
        'pd': pd,
        '_duckdb_add_missing_columns': lambda *a, **k: None,
        '_duckdb_table_columns': lambda *a, **k: set(),
    }
    exec(code, globals_dict)
    return globals_dict['_duckdb_upsert_df']


def test_duckdb_upsert_df_missing_key_raises_value_error():
    _duckdb_upsert_df = load_duckdb_upsert()
    df = pd.DataFrame({'a': [1, 2]})
    with pytest.raises(ValueError) as excinfo:
        _duckdb_upsert_df(None, 'dummy', df, ['a', 'b'])
    assert 'b' in str(excinfo.value)
