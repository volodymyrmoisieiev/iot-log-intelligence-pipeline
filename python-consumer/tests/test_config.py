import pytest

from config import (
    DEFAULT_CONSUMER_PROGRESS_INTERVAL,
    load_config,
)


def test_load_config_defaults_progress_interval(monkeypatch):
    monkeypatch.delenv("CONSUMER_PROGRESS_INTERVAL", raising=False)

    config = load_config()

    assert config.consumer_progress_interval == DEFAULT_CONSUMER_PROGRESS_INTERVAL


def test_load_config_accepts_explicit_progress_interval(monkeypatch):
    monkeypatch.setenv("CONSUMER_PROGRESS_INTERVAL", "250")

    config = load_config()

    assert config.consumer_progress_interval == 250


@pytest.mark.parametrize("value", ["abc", "-1", "0"])
def test_load_config_rejects_invalid_progress_interval(monkeypatch, value):
    monkeypatch.setenv("CONSUMER_PROGRESS_INTERVAL", value)

    with pytest.raises(ValueError, match="CONSUMER_PROGRESS_INTERVAL"):
        load_config()
