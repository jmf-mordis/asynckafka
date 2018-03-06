import logging

import asyncio

from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka import exceptions


logger = logging.getLogger('asynckafka')


def check_rdkafka_version():
    """
    Check that the proper rdkafka version is installed, it throws
    a ImportError then it is not.

    Raises:
        ImportError: Incorrect rdkafka version installed.
    """
    version_str = crdk.rd_kafka_version_str()
    mayor, minor, revision = str(version_str).split(".")
    if mayor != "0" and minor != "11":
        raise ImportError(
            f"Rdkafka 0.11.X is required, actual: {version_str}"
        )


def _parse_settings(config: dict) -> dict:
    return {
        key.replace("_", "."): value
        for key, value in config.items()
    }


def _encode_settings(settings: dict) -> dict:
    return {
        key.encode(): value.encode()
        for key, value in settings.items()
    }


def parse_and_encode_settings(settings: dict) -> dict:
    parsed_settings = _parse_settings(settings)
    return _encode_settings(parsed_settings)


def parse_rd_kafka_conf_response(
        conf_respose: int, key: bytes, value: bytes):
    key_str = key.decode()
    value_str = value.decode()
    if conf_respose == crdk.RD_KAFKA_CONF_OK:
        logger.info(f"Correctly configured rdkafka {key_str} with value "
                     f"{value_str}")
    elif conf_respose == crdk.RD_KAFKA_CONF_INVALID:
        err_str = f"Invalid {key_str} setting with value: {value_str}"
        logger.error(err_str)
        raise exceptions.InvalidSetting(err_str)
    elif conf_respose == crdk.RD_KAFKA_CONF_UNKNOWN:
        err_str = f"Unknown {value_str} setting with value {value_str}"
        logger.error(err_str)
        raise exceptions.UnknownSetting(err_str)


async def periodic_rd_kafka_poll(
        long rk_addr, loop: asyncio.AbstractEventLoop):
    rk_ptr = <crdk.rd_kafka_t*> rk_addr
    while True:
        await asyncio.sleep(1, loop=loop)
        crdk.rd_kafka_poll(rk_ptr, 0)
