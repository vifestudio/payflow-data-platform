"""Shared helpers for the PayFlow test suite.

The service folders are not Python packages (some contain hyphens, e.g.
sources/fx-api), so modules are loaded straight from their file path. Each module
is loaded once per session: serve.py and consumer.py register Prometheus metrics
at import time and registering them twice raises an error.
"""
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    if name in sys.modules:
        return sys.modules[name]
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
