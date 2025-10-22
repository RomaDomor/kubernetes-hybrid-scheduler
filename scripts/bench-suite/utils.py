import time
from pathlib import Path
from typing import Dict, Any, List, Optional
import yaml

def log(msg: str):
    """Prints a log message with a timestamp."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def ensure_dir(path: Path):
    """Ensures that a directory exists."""
    path.mkdir(parents=True, exist_ok=True)

def file_exists(path: Path) -> bool:
    """Checks if a path is an existing file."""
    return path.exists() and path.is_file()

def read_yaml_multi(path: Path) -> List[Dict[str, Any]]:
    """Reads all documents from a YAML file."""
    if not file_exists(path):
        return []
    with path.open() as f:
        return [doc for doc in yaml.safe_load_all(f) if doc]

def get_annotations(doc: Dict[str, Any]) -> Dict[str, str]:
    """Extracts annotations from a Kubernetes object document, checking templates."""
    ann = {}
    meta = doc.get("metadata", {})
    a1 = meta.get("annotations", {}) or {}
    ann.update(a1)
    spec = doc.get("spec", {}) or {}
    tmpl = spec.get("template", {}) or {}
    tmeta = tmpl.get("metadata", {}) or {}
    a2 = tmeta.get("annotations", {}) or {}
    ann.update(a2)
    return {k: str(v) for k, v in ann.items()}

def to_int(val: Optional[str]) -> Optional[int]:
    """Safely converts a string value to an integer."""
    if val is None or val == "":
        return None
    try:
        return int(str(val))
    except (ValueError, TypeError):
        try:
            return int(float(str(val)))
        except (ValueError, TypeError):
            return None