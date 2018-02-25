import logging

from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.settings cimport debug


logger = logging.getLogger('asynckafka')


cdef inline Message message_factory(crdk.rd_kafka_message_t *rk_message):
    message = Message()
    if _is_error(rk_message):
        message.error = 1
    else:
        _copy_data_to_message(message, rk_message)
        crdk.rd_kafka_message_destroy(rk_message)
    return message


cdef inline Message _copy_data_to_message(
        Message message, crdk.rd_kafka_message_t *rk_message):
    message.error = 0

    payload_ptr = <char*> rk_message.payload
    message.payload = payload_ptr[:rk_message.len]

    key_ptr = <char*> rk_message.key
    message.key = key_ptr[:rk_message.key_len]

    message.topic = bytes(crdk.rd_kafka_topic_name(rk_message.rkt)).decode()
    if rk_message.offset:
        message.offset = rk_message.offset

    return message


cdef inline char _is_error(crdk.rd_kafka_message_t *rk_message):
    if rk_message.err:
        if rk_message.err == crdk.RD_KAFKA_RESP_ERR__PARTITION_EOF:
            if debug: logger.info("Partition EOF")
            return 1
        elif rk_message.rkt:
            err_message_str = str(crdk.rd_kafka_message_errstr(
                rk_message))
            topic = str(crdk.rd_kafka_topic_name(rk_message.rkt))
            logger.error(
                f"Consumer error in kafka topic {topic}, "
                f"partition {rk_message.partition}, "
                f"offset {rk_message.offset} "
                f"error info: {err_message_str}"
            )
            return 1
        else:
            err_str = crdk.rd_kafka_err2str(rk_message.err)
            err_message_str = crdk.rd_kafka_message_errstr(rk_message)
            logger.error(
                f"Consumer error {err_str} {err_message_str}"
            )
            return 1
    else:
        if debug:
            payload_ptr = <char*> rk_message.payload
            payload_len = rk_message.len
            topic = crdk.rd_kafka_topic_name(rk_message.rkt)
            logger.debug(
                f"Consumed message in thread of topic {topic} "
                f"with payload: {payload_ptr[:payload_len]}"
            )
        return 0


cdef class Message:
    pass
