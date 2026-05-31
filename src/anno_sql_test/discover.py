from pathlib import Path


def discover_sql_files(path: Path, pattern: str = "*.sql") -> list[Path]:
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")
    if path.is_file():
        return [path]
    files = sorted(path.rglob(pattern))
    return list(files)
