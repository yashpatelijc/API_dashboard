import ast
import types
import pathlib
import pandas as pd
import duckdb


def load_upsert():
    code = pathlib.Path('qh_api_dashboard.py').read_text()
    mod_ast = ast.parse(code)
    needed = [n for n in mod_ast.body if isinstance(n, ast.FunctionDef) and n.name in ('_duckdb_upsert_df','_duckdb_add_missing_columns','_duckdb_table_columns')]
    subset = types.ModuleType('subset')
    subset.pd = pd
    subset.logger = types.SimpleNamespace(warning=lambda *a, **k: None)
    for fn in needed:
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'qh_api_dashboard.py', 'exec'), subset.__dict__)
    return subset


def test_duplicate_ids_fallback(monkeypatch):
    mod = load_upsert()
    con = duckdb.connect(database=':memory:')
    con.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);')
    df = pd.DataFrame({'id':[1,1], 'val':[10,11]})

    class BoomConn:
        def __init__(self, real):
            self.real = real
            self.first = True
        def execute(self, sql, *args, **kwargs):
            if self.first and 'INSERT OR REPLACE' in sql:
                self.first = False
                raise duckdb.Error('force fallback')
            return self.real.execute(sql, *args, **kwargs)
        def register(self, *a, **k):
            return self.real.register(*a, **k)
        def unregister(self, *a, **k):
            return self.real.unregister(*a, **k)

    boom = BoomConn(con)
    mod._duckdb_upsert_df(boom, 't', df, ['id'])
    res = con.execute('SELECT * FROM t').fetchall()
    assert res == [(1, 11)]
