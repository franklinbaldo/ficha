from pathlib import Path

path = Path("etl/tests/test_registry.py")
text = path.read_text()
needle = '''            "    ['a.csv'],\\n"
            "    delim=';',\\n"
'''
replacement = '''            "    ['a.csv'],\\n"
            "    auto_detect=false,\\n"
            "    delim=';',\\n"
'''
if needle not in text:
    raise SystemExit("pinned read_csv SQL expectation insertion point not found")
path.write_text(text.replace(needle, replacement, 1))
