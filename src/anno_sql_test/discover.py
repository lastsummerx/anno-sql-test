import logging
from pathlib import Path


def discover_sql_files(path: Path, pattern: str = "*.sql") -> list[Path]:
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")
    if path.is_file():
        logging.debug("Using single file: %s", path)
        return [path]
    files = sorted(path.rglob(pattern))
    logging.info("Discovered %d SQL file(s) in %s", len(files), path)
    return list(files)
