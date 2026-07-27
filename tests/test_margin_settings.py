from raspberry_executor import margin_settings


def test_default_margin_leverage_is_ten(monkeypatch, tmp_path):
    monkeypatch.setattr(margin_settings, "ENV_PATH", tmp_path / ".env")

    assert margin_settings.margin_multiplier() == 10.0


def test_margin_leverage_attempts_descend_from_ten_to_two():
    assert margin_settings.margin_leverage_attempts() == (10, 9, 8, 7, 6, 5, 4, 3, 2)


def test_margin_multiplier_is_capped_at_ten(monkeypatch, tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("MARGIN_MAX_MULTIPLIER=25\n")
    monkeypatch.setattr(margin_settings, "ENV_PATH", env_path)

    assert margin_settings.margin_multiplier() == 10.0
