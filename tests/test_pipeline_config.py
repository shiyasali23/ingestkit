import pytest

from ingestkit.pipeline_config import DATABASE_URL_VARIABLE, PSEUDONYM_SALT_VARIABLE, load_pipeline_config
from ingestkit.pipeline_errors import ConfigError


@pytest.fixture(autouse=True)
def pseudonym_salt(monkeypatch):
    monkeypatch.setenv(PSEUDONYM_SALT_VARIABLE, "salt-for-config-tests")


def write_config_file(tmp_path, config_text: str):
    config_path = tmp_path / "pipeline_config.yaml"
    config_path.write_text(config_text)
    return config_path


def test_config_loads_with_defaults(tmp_path):
    config_path = write_config_file(tmp_path, "source_path: ./in\ndestination_path: ./out\n")
    loaded_config = load_pipeline_config(str(config_path))
    assert loaded_config.batch_size_gb == 4
    assert loaded_config.batch_size_bytes == 4 * 1024**3
    assert loaded_config.chunk_size_bytes == 8 * 1024**2


def test_config_database_url_comes_from_environment(tmp_path, monkeypatch):
    config_path = write_config_file(tmp_path, "source_path: ./in\ndestination_path: ./out\n")
    monkeypatch.setenv(DATABASE_URL_VARIABLE, "sqlite:///from_environment.db")
    assert load_pipeline_config(str(config_path)).state_database_url == "sqlite:///from_environment.db"


def test_config_rejects_negative_batch_size(tmp_path):
    config_path = write_config_file(
        tmp_path, "source_path: ./in\ndestination_path: ./out\nbatch_size_gb: -1\n"
    )
    with pytest.raises(ConfigError):
        load_pipeline_config(str(config_path))


def test_config_rejects_missing_source(tmp_path):
    config_path = write_config_file(tmp_path, "destination_path: ./out\n")
    with pytest.raises(ConfigError):
        load_pipeline_config(str(config_path))


def test_config_rejects_pattern_without_group(tmp_path):
    config_path = write_config_file(
        tmp_path, "source_path: ./in\ndestination_path: ./out\ncase_id_pattern: '^C'\n"
    )
    with pytest.raises(ConfigError):
        load_pipeline_config(str(config_path))


def test_config_reports_missing_file(tmp_path):
    with pytest.raises(ConfigError):
        load_pipeline_config(str(tmp_path / "missing.yaml"))


def test_config_requires_pseudonym_salt(tmp_path, monkeypatch):
    monkeypatch.delenv(PSEUDONYM_SALT_VARIABLE)
    config_path = write_config_file(tmp_path, "source_path: ./in\ndestination_path: ./out\n")
    with pytest.raises(ConfigError, match=PSEUDONYM_SALT_VARIABLE):
        load_pipeline_config(str(config_path))


def test_salt_is_left_out_of_saved_snapshot(tmp_path):
    config_path = write_config_file(tmp_path, "source_path: ./in\ndestination_path: ./out\n")
    assert "pseudonym_salt" not in load_pipeline_config(str(config_path)).safe_snapshot()


def test_gene_panel_is_upper_cased(tmp_path):
    config_path = write_config_file(
        tmp_path, "source_path: ./in\ndestination_path: ./out\ngene_panel: [sf3b1, ' tp53 ']\n"
    )
    assert load_pipeline_config(str(config_path)).gene_panel == ["SF3B1", "TP53"]
