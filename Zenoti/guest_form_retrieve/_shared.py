"""Load the parent Zenoti environment and its existing ZConnect helpers."""

import importlib.util
import sys
from pathlib import Path

from dotenv import load_dotenv


ZENOTI_ROOT = Path(__file__).resolve().parent.parent
ZCONNECT_ROOT = ZENOTI_ROOT / "zconnect"


def load_shared_environment():
    """Environment variables take precedence over the parent project's files."""
    for filename in (".env", "env"):
        load_dotenv(ZENOTI_ROOT / filename, override=False)


def load_zconnect_module(name):
    """Import the sibling helper by path, without ambiguous top-level imports."""
    if name not in {"_client", "login_headers"}:
        raise ValueError(f"Unsupported ZConnect helper: {name}")
    module_name = f"_guest_form_retrieve_zconnect_{name}"
    if module_name in sys.modules:
        return sys.modules[module_name]

    source = ZCONNECT_ROOT / f"{name}.py"
    if not source.is_file():
        raise ImportError(f"Required shared helper was not found: {source}")
    spec = importlib.util.spec_from_file_location(module_name, source)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load shared helper: {source}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    return module
