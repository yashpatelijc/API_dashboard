import sys
import types
from pathlib import Path

import duckdb
import pandas as pd

# Stub heavy optional dependencies so executing the snippet doesn't fail
sys.modules.setdefault('requests', types.SimpleNamespace(get=lambda *a, **k: None))
class _SessionState(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
class _StreamlitModule:
    def __init__(self):
        self.session_state = _SessionState()
    def __call__(self, *a, **k):
        return self
    def __getattr__(self, name):
        if name in ('cache_data','cache_resource'):
            return lambda *a, **k: (lambda f: f)
        return self
    def __getitem__(self, key):
        return self
    def __iter__(self):
        yield self
    def __enter__(self):
        return self
    def __exit__(self, *exc):
        return False
sys.modules.setdefault('streamlit', _StreamlitModule())
sys.modules.setdefault('plotly', types.SimpleNamespace(graph_objects=types.SimpleNamespace(Figure=object)))
sys.modules.setdefault('plotly.graph_objects', types.SimpleNamespace(Figure=object))
sys.modules.setdefault('streamlit_autorefresh', types.SimpleNamespace(st_autorefresh=lambda *a, **k: None))

# Execute only the portion of qh_api_dashboard needed for upsert helper
source = Path('qh_api_dashboard.py').read_text().split('# Render settings now')[0]
namespace = {}
exec(source, namespace)
_duckdb_upsert_df = namespace['_duckdb_upsert_df']

class FailInsertConnection:
    """DuckDB connection wrapper that forces fallback path."""
    def __init__(self, con):
        self._con = con
    def execute(self, query, *args, **kwargs):
        if query.strip().startswith('INSERT OR REPLACE INTO'):
            raise duckdb.Error('INSERT OR REPLACE unsupported')
        return self._con.execute(query, *args, **kwargs)
    def register(self, name, obj):
        return self._con.register(name, obj)
    def unregister(self, name):
        return self._con.unregister(name)

def test_duplicate_ids_do_not_raise(monkeypatch):
    con = duckdb.connect()
    con.execute('CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);')
    df = pd.DataFrame({'id':[1,1], 'val':[10,20]})
    monkeypatch.setattr(pd.DataFrame, 'drop_duplicates', lambda self, subset=None, keep='last': self)
    failing_con = FailInsertConnection(con)
    _duckdb_upsert_df(failing_con, 't', df, ['id'])
    rows = con.execute('SELECT * FROM t').fetchall()
    assert len(rows) == 1 and rows[0][0] == 1
