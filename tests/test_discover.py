import tempfile
from pathlib import Path

from anno_sql_test.discover import discover_sql_files


def test_discover_single_file():
    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False, mode="w") as f:
        f.write("select 1;")
        fpath = f.name
    try:
        result = discover_sql_files(Path(fpath))
        assert len(result) == 1
        assert result[0] == Path(fpath)
    finally:
        Path(fpath).unlink()


def test_discover_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = Path(tmpdir)
        (d / "a.sql").write_text("select 1;")
        (d / "sub").mkdir()
        (d / "sub" / "b.sql").write_text("select 2;")
        (d / "notes.txt").write_text("hello")
        result = discover_sql_files(d)
        assert len(result) == 2
        assert any(p.name == "a.sql" for p in result)
        assert any(p.name == "b.sql" for p in result)


def test_discover_custom_pattern():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = Path(tmpdir)
        (d / "test.sql").write_text("select 1;")
        (d / "test.SQL").write_text("select 2;")
        result = discover_sql_files(d, pattern="*.SQL")
        assert len(result) == 1


def test_discover_nonexistent_path():
    import pytest
    with pytest.raises(FileNotFoundError):
        discover_sql_files(Path("/nonexistent/path.sql"))
