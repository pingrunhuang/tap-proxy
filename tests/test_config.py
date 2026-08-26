import pytest

from config import Settings


def test_default_ports_are_reserved_for_tap_proxy():
    settings = Settings()
    assert settings.zmq_pub_port == 5575
    assert settings.zmq_rep_port == 5576


@pytest.mark.parametrize(
    "settings,message",
    [
        (Settings(zmq_bind_host=""), "ZMQ_BIND_HOST"),
        (Settings(zmq_pub_port=-1), "ZMQ_PUB_PORT"),
        (Settings(zmq_rep_port=70000), "ZMQ_REP_PORT"),
        (Settings(zmq_pub_port=5575, zmq_rep_port=5575), "must be different"),
        (Settings(publish_queue_size=0), "ZMQ_PUBLISH_QUEUE_SIZE"),
    ],
)
def test_invalid_settings_are_rejected(settings, message):
    with pytest.raises(ValueError, match=message):
        settings.validate()


def test_native_tap_config_is_only_required_for_real_session():
    Settings().validate()
    with pytest.raises(ValueError, match="TAP_MD_HOST"):
        Settings().validate(require_tap=True)


def test_td_only_mode_only_requires_td_configuration():
    settings = Settings(
        enable_md=False,
        td_host="td.example",
        td_port=10002,
        td_user_id="td-user",
        td_password="td-password",
        td_auth_code="td-auth",
    )
    settings.validate(require_tap=True)


def test_td_only_mode_rejects_startup_symbols():
    with pytest.raises(ValueError, match="TAP_SYMBOLS must be empty"):
        Settings(enable_md=False, initial_symbols=["COMEX:F:GC:2608"]).validate()
