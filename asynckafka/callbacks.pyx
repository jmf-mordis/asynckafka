import logging

import asyncio

from asynckafka.exceptions import KafkaError
from asynckafka.includes cimport c_rd_kafka as crdk


logger = logging.getLogger('asynckafka')


_error_callback = None
_name_to_callback_error = {}


cdef void cb_logger(const crdk.rd_kafka_t *rk, int level, const char *fac,
                    const char *buf):
    fac_str = bytes(fac).decode()
    buf_str = bytes(buf).decode()
    if level in {1, 2}:
        logger.critical(f"{fac_str}:{buf_str}")
    elif level == 3:
        logger.error(f"{fac_str}:{buf_str)}")
    elif level in {4, 5}:
        logger.info(f"{fac_str}:{buf_str}")
    elif level in {6, 7}:
        logger.debug(f"{fac_str}:{buf_str}")


cdef void cb_error(crdk.rd_kafka_t *rk, int err, const char *reason,
                   void *opaque):
    err_enum = <crdk.rd_kafka_resp_err_t> err
    rd_name_str = bytes(crdk.rd_kafka_name(rk)).decode()
    error_str = bytes(crdk.rd_kafka_err2str(err_enum)).decode()
    reason_str = bytes(reason).decode()
    logger.error(f"Error callback. {rd_name_str}, {error_str}, {reason_str}")

    try:
        consumer_or_producer = _name_to_callback_error[rd_name_str]
    except KeyError:
        logger.error("Error callback of not registered producer or consumer")
    else:
        kafka_error = KafkaError(rk_name=rd_name_str, error_code=err,
                                 error_str=error_str, reason=reason,
                                 consumer_or_producer=consumer_or_producer)
        try:
            asyncio.run_coroutine_threadsafe(
                consumer_or_producer.error_callback(kafka_error),
                loop=consumer_or_producer.loop
            )
        except Exception:
            logger.exception("Unexpected exception opening a error"
                             " callback corutine.")


def register_error_callback(consumer_or_producer, name):
    """
    Internal method used by the consumer and producer to register themselves.
    Args:
        consumer_or_producer (Union[asynckafka.Producer,
            asynckafka.Consumer]):
        name (str):
    """
    logger.info(f"Registering error callback of {name}")
    _name_to_callback_error[name] = consumer_or_producer


def unregister_error_callback(name):
    """
    Internal method used by the consumer and producer to unregister themselves.
    Args:
        name (str):
    """
    logger.info(f"Unregistering error callback of {name}")
    del _name_to_callback_error[name]


cdef inline log_partition_list(
        crdk.rd_kafka_topic_partition_list_t *partitions):
    string = "List of partitions:"
    for i in range(partitions.cnt):
        topic = partitions.elems[i].topic
        partition = partitions.elems[i].partition
        offset = partitions.elems[i].offset
        string += f" [Topic: {str(topic)}, " \
                  f"Partition: {str(partition)}, " \
                  f"Offset: {str(offset)}]"
    logger.debug(string)


cdef void cb_rebalance(crdk.rd_kafka_t *rk, crdk.rd_kafka_resp_err_t err,
        crdk.rd_kafka_topic_partition_list_t *partitions, void *opaque):
    logger.debug("Consumer group rebalance")
    if err == crdk.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        logger.debug("New partitions assigned")
        log_partition_list(partitions)
        crdk.rd_kafka_assign(rk, partitions)
    elif err == crdk.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        logger.debug("Revoked Partitions")
        log_partition_list(partitions)
        crdk.rd_kafka_assign(rk, NULL)
    else:
        err_str = crdk.rd_kafka_err2str(err)
        logger.error(
            f"Error in rebalance callback, Revoked partitions {err_str}"
        )
        crdk.rd_kafka_assign(rk, NULL)
