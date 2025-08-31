from functools import lru_cache
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, StrictUndefined

BASE_DIR = Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "sql"

env = Environment(
    loader=FileSystemLoader(str(SQL_DIR)),
    undefined=StrictUndefined,
    autoescape=False,           
    trim_blocks=True,
    lstrip_blocks=True,
)

@lru_cache(maxsize=None)
def load_sql(name: str) -> str:
    """Load raw .sql text by basename (without extension)."""
    path = SQL_DIR / f"{name}.sql"
    if not path.exists():
        raise FileNotFoundError(f"SQL not found: {path}")
    return path.read_text(encoding="utf-8")

def render_sql(name: str, **params) -> str:
    """Render a Jinja-templated .sql with params."""
    template = env.get_template(f"{name}.sql")
    return template.render(**params)