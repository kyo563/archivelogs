from pathlib import Path


def test_core_modules_do_not_import_streamlit():
    for path in Path("archivelogs").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "import streamlit" not in text
