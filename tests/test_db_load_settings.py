import ast
import types
import pathlib
import json
import pytest


def load_subset(tmp_path):
    code = pathlib.Path('qh_api_dashboard.py').read_text()
    mod_ast = ast.parse(code)
    needed = [n for n in mod_ast.body if isinstance(n, ast.FunctionDef) and n.name in ('_duckdb_path', 'db_load_settings')]
    subset = types.ModuleType('subset')
    subset.__dict__['os'] = __import__('os')
    subset.__dict__['json'] = json
    # minimal ensure_dir
    subset.ensure_dir = lambda p: __import__('os').makedirs(p, exist_ok=True)
    # simple session_state
    class Session(dict):
        def __getattr__(self, name):
            return self[name]
        def __setattr__(self, name, value):
            self[name] = value
    subset.st = types.SimpleNamespace(session_state=Session(cfg={'store_root': str(tmp_path), 'db_filename': 'settings.duckdb'},
                                                            store_root=str(tmp_path)))
    # placeholders to be patched
    subset._duckdb_connect = lambda: None
    subset._duckdb_init = lambda con: None
    class DuckError(Exception):
        pass
    subset.duckdb = types.SimpleNamespace(Error=DuckError)
    subset.logger = types.SimpleNamespace(warning=lambda *a, **k: None)
    for fn in needed:
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'qh_api_dashboard.py', 'exec'), subset.__dict__)
    return subset


def test_expected_duckdb_error(tmp_path, monkeypatch):
    mod = load_subset(tmp_path)

    class Conn:
        def close(self):
            pass
    monkeypatch.setattr(mod, '_duckdb_connect', lambda: Conn())

    def bad_init(con):
        raise mod.duckdb.Error('boom')
    monkeypatch.setattr(mod, '_duckdb_init', bad_init)

    pathlib.Path(mod._duckdb_path()).touch()
    assert mod.db_load_settings() is None


def test_expected_json_error(tmp_path, monkeypatch):
    mod = load_subset(tmp_path)

    class Conn:
        def execute(self, sql):
            class Cur:
                def fetchone(self):
                    return ['not json']
            return Cur()
        def close(self):
            pass
    monkeypatch.setattr(mod, '_duckdb_connect', lambda: Conn())
    pathlib.Path(mod._duckdb_path()).touch()
    assert mod.db_load_settings() is None


def test_unexpected_error_propagates(tmp_path, monkeypatch):
    mod = load_subset(tmp_path)

    class Conn:
        def close(self):
            pass
    monkeypatch.setattr(mod, '_duckdb_connect', lambda: Conn())
    def boom(con):
        raise RuntimeError('unexpected')
    monkeypatch.setattr(mod, '_duckdb_init', boom)
    pathlib.Path(mod._duckdb_path()).touch()
    with pytest.raises(RuntimeError):
        mod.db_load_settings()
