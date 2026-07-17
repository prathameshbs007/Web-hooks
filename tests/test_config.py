from relay.config import Settings


def test_defaults():
    s = Settings(_env_file=None)
    assert s.stream_shards == 8
    assert s.max_attempts == 7
    assert s.delivery_timeout_seconds == 10
    assert s.default_tenant_rate_per_sec == 50


def test_env_override(monkeypatch):
    monkeypatch.setenv("MAX_ATTEMPTS", "3")
    monkeypatch.setenv("REDIS_URL", "redis://elsewhere:6379/1")
    s = Settings(_env_file=None)
    assert s.max_attempts == 3
    assert s.redis_url == "redis://elsewhere:6379/1"
