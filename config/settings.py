"""
config/settings.py
==================
Loads pipeline_config.yaml, expands ${ENV_VAR} placeholders from .env,
and exposes a single `settings` object used everywhere.

Usage:
    from config.settings import settings
    settings.database.host
    settings.sla.first_response_minutes
"""

import os
import re
import yaml
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if it exists (local dev). In prod, env vars are set externally.
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE)


def _expand_env_vars(value: str) -> str:
    """Replace ${VAR_NAME} with the actual environment variable value."""
    def replacer(match):
        var_name = match.group(1)
        env_value = os.environ.get(var_name)
        if env_value is None:
            # Not a hard failure — return placeholder so missing creds are visible
            return f"<MISSING:{var_name}>"
        return env_value

    return re.sub(r"\$\{(\w+)\}", replacer, value)


def _resolve(obj):
    """Recursively walk the parsed YAML and expand env vars in string values."""
    if isinstance(obj, dict):
        return {k: _resolve(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve(item) for item in obj]
    if isinstance(obj, str):
        return _expand_env_vars(obj)
    return obj


class _Namespace:
    """
    Converts a nested dict into attribute-accessible object.
    Allows:  settings.database.host  instead of  settings['database']['host']
    """

    def __init__(self, data: dict):
        for key, value in data.items():
            if isinstance(value, dict):
                setattr(self, key, _Namespace(value))
            else:
                setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __repr__(self):
        return f"_Namespace({self.__dict__})"


def _load_config() -> _Namespace:
    config_path = Path(__file__).resolve().parent / "pipeline_config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            "Make sure pipeline_config.yaml is in the config/ directory."
        )

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    resolved = _resolve(raw)
    return _Namespace(resolved)


# Singleton — imported once, reused everywhere
settings = _load_config()
