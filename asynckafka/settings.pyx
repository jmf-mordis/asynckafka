CONSUMER_RD_KAFKA_POLL_PERIOD_SECONDS = 0.1
PRODUCER_RD_KAFKA_POLL_PERIOD_SECONDS = 0.1

cdef char debug = 0

def set_debug(set_debug: bool):
    global debug
    debug = 1 if set_debug else 0

def is_in_debug():
    return debug == 1
