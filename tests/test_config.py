from utils.config import load_config


def test_config_loads_defaults():
    config = load_config()
    assert "kafka" in config
    assert "spark" in config
    assert "snowflake" in config

