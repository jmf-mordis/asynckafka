CONSUMER_RD_KAFKA_POLL_PERIOD_SECONDS = 0.1
PRODUCER_RD_KAFKA_POLL_PERIOD_SECONDS = 0.1

cdef char debug = 0

def set_debug(set_debug: bool):
    """
    Set the debug status in asynckafka. By default some debug logging calls
    in the performance critical path are disabled by default. Activating the
    debug mode has a performance cost even if you do not have the python
    logging enabled, since the calls to the logger are made and the strings
    for it are built.

    Args:
        set_debug (bool): Enable of disable the debug.
    """
    global debug
    debug = 1 if set_debug else 0

def is_in_debug():
    """
    Returns:
        bool: True if debug False if not.
    """
    return debug == 1
