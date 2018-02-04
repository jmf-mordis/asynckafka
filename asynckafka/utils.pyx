cimport rdkafka


def check_rdkafka_version():
    """
    Check that the proper rdkafka version is installed, it throws
    a ImportError then it is not.
    """
    version_str = rdkafka.rd_kafka_version_str()
    mayor, minor, revision = str(version_str).split(".")
    if mayor != "0" and minor != "11":
        raise ImportError(f"Rdkafka 0.11.X is required, actual: {version_str}")

