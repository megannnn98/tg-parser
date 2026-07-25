from __future__ import annotations

from pathlib import Path

from comment_cleaner.config import Config, load_config, load_dictionary


def test_load_config_defaults():
    cfg = load_config(None)
    assert cfg.input_type == "jsonl"
    assert cfg.duplicate_mode == "mark"
    assert cfg.unicode_form == "NFC"
    assert cfg.max_repeated_letters == 3


def test_load_config_from_yaml(temp_dir):
    yaml_content = """
input:
  type: sqlite
  path: test.db
duplicates:
  mode: collapse
normalization:
  max_repeated_letters: 5
processing_version: "2.0.0"
"""
    path = temp_dir / "config.yaml"
    path.write_text(yaml_content)
    cfg = load_config(path)
    assert cfg.input_type == "sqlite"
    assert cfg.input_path == "test.db"
    assert cfg.duplicate_mode == "collapse"
    assert cfg.max_repeated_letters == 5
    assert cfg.processing_version == "2.0.0"


def test_config_partial_override():
    data = {
        "input": {"type": "jsonl"},
        "duplicates": {"mode": "keep"},
    }
    cfg = Config(data)
    assert cfg.input_type == "jsonl"
    assert cfg.duplicate_mode == "keep"
    assert cfg.unicode_form == "NFC"  # default still preserved


def test_config_get_with_default():
    cfg = load_config(None)
    result = cfg.get("nonexistent", "key", default="fallback")
    assert result == "fallback"


def test_load_political_slang_dictionary():
    dict_path = Path("dictionaries/political_slang.yaml")
    if not dict_path.exists():
        import pytest

        pytest.skip("Dictionary file not found")
    data = load_dictionary(dict_path)
    assert isinstance(data, dict)
    assert "вата" in data
    assert data["вата"]["category"] == "political_slang"
