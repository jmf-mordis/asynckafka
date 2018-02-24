import logging

from asynckafka.exceptions import KafkaError
from asynckafka.includes cimport c_rd_kafka as crdk


logger = logging.getLogger('asynckafka')


_error_callback = None


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
    kafka_error = KafkaError(rk_name=rd_name_str, error_code=err,
                             error_str=error_str, reason=reason)
    logger.error(f"Error callback! {rd_name_str}, {error_str} {reason_str}")
    try:
        if _error_callback:
            _error_callback(kafka_error)
    except Exception:
        logger.error(
            "Unexpected exception calling error callback",
            exc_info=True
        )


def set_error_callback(func):
    global _error_callback
    _error_callback = func


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
