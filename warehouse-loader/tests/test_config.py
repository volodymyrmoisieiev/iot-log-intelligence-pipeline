import pytest

from config import (
    DEFAULT_WAREHOUSE_LOADER_BATCH_SIZE,
    DEFAULT_WAREHOUSE_LOADER_PROGRESS_INTERVAL,
    load_config,
)


def test_load_config_defaults_progress_interval(monkeypatch):
    monkeypatch.delenv("WAREHOUSE_LOADER_PROGRESS_INTERVAL", raising=False)

    config = load_config()

    assert (
        config.warehouse_loader_progress_interval
        == DEFAULT_WAREHOUSE_LOADER_PROGRESS_INTERVAL
    )


def test_load_config_accepts_explicit_progress_interval(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_LOADER_PROGRESS_INTERVAL", "250")

    config = load_config()

    assert config.warehouse_loader_progress_interval == 250


@pytest.mark.parametrize("value", ["abc", "-1", "0"])
def test_load_config_rejects_invalid_progress_interval(monkeypatch, value):
    monkeypatch.setenv("WAREHOUSE_LOADER_PROGRESS_INTERVAL", value)

    with pytest.raises(ValueError, match="WAREHOUSE_LOADER_PROGRESS_INTERVAL"):
        load_config()


def test_load_config_defaults_batch_size(monkeypatch):
    monkeypatch.delenv("WAREHOUSE_LOADER_BATCH_SIZE", raising=False)

    config = load_config()

    assert config.warehouse_loader_batch_size == DEFAULT_WAREHOUSE_LOADER_BATCH_SIZE


def test_load_config_accepts_explicit_batch_size(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_LOADER_BATCH_SIZE", "250")

    config = load_config()

    assert config.warehouse_loader_batch_size == 250


@pytest.mark.parametrize("value", ["abc", "-1", "0"])
def test_load_config_rejects_invalid_batch_size(monkeypatch, value):
    monkeypatch.setenv("WAREHOUSE_LOADER_BATCH_SIZE", value)

    with pytest.raises(ValueError, match="WAREHOUSE_LOADER_BATCH_SIZE"):
        load_config()
