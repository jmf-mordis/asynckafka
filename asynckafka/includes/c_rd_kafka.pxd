from libc.stdint cimport int32_t, int64_t

cdef extern from "librdkafka/rdkafka.h":
    ctypedef struct rd_kafka_t
    ctypedef struct rd_kafka_topic_t
    ctypedef struct rd_kafka_conf_t
    ctypedef struct rd_kafka_topic_conf_t
    ctypedef struct rd_kafka_queue_t

    ctypedef enum rd_kafka_type_t:
        RD_KAFKA_PRODUCER
        RD_KAFKA_CONSUMER

    ctypedef enum rd_kafka_conf_res_t:
        RD_KAFKA_CONF_UNKNOWN = -2
        RD_KAFKA_CONF_INVALID = -1
        RD_KAFKA_CONF_OK = 0

    rd_kafka_conf_t *rd_kafka_conf_new()

    rd_kafka_conf_res_t rd_kafka_conf_set(
            rd_kafka_conf_t *conf,
            const char *name,
            const char *value,
            char *errstr, size_t errstr_size
    )

    void rd_kafka_conf_set_dr_msg_cb(
            rd_kafka_conf_t *conf,
            void (*dr_msg_cb)(
                    rd_kafka_t *rk,
                    const rd_kafka_message_t *rkmessage,
                    void *opaque
            )
    )

    void rd_kafka_conf_set_log_cb(
            rd_kafka_conf_t *conf,
			void (*log_cb) (
                    const rd_kafka_t *rk,
                    int level,
                    const char *fac,
                    const char *buf
            )
    )

    void rd_kafka_conf_set_error_cb(
            rd_kafka_conf_t *conf,
			void  (*error_cb) (
                    rd_kafka_t *rk,
                    int err,
					const char *reason,
					void *opaque
            )
    )

    ctypedef enum rd_kafka_resp_err_t:
        RD_KAFKA_RESP_ERR__BEGIN = -200
        RD_KAFKA_RESP_ERR__BAD_MSG = -199
        RD_KAFKA_RESP_ERR__BAD_COMPRESSION = -198
        RD_KAFKA_RESP_ERR__DESTROY = -197
        RD_KAFKA_RESP_ERR__FAIL = -196
        RD_KAFKA_RESP_ERR__TRANSPORT = -195
        RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE = -194
        RD_KAFKA_RESP_ERR__RESOLVE = -193
        RD_KAFKA_RESP_ERR__MSG_TIMED_OUT = -192
        RD_KAFKA_RESP_ERR__PARTITION_EOF = -191
        RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION = -190
        RD_KAFKA_RESP_ERR__FS = -189
        RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC = -188
        RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN = -187
        RD_KAFKA_RESP_ERR__INVALID_ARG = -186
        RD_KAFKA_RESP_ERR__TIMED_OUT = -185
        RD_KAFKA_RESP_ERR__QUEUE_FULL = -184
        RD_KAFKA_RESP_ERR__ISR_INSUFF = -183
        RD_KAFKA_RESP_ERR__NODE_UPDATE = -182
        RD_KAFKA_RESP_ERR__SSL = -181
        RD_KAFKA_RESP_ERR__WAIT_COORD = -180
        RD_KAFKA_RESP_ERR__UNKNOWN_GROUP = -179
        RD_KAFKA_RESP_ERR__IN_PROGRESS = -178
        RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS = -177
        RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION = -176
        RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS = -175
        RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS = -174
        RD_KAFKA_RESP_ERR__CONFLICT = -173
        RD_KAFKA_RESP_ERR__STATE = -172
        RD_KAFKA_RESP_ERR__UNKNOWN_PROTOCOL = -171
        RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED = -170
        RD_KAFKA_RESP_ERR__AUTHENTICATION = -169
        RD_KAFKA_RESP_ERR__NO_OFFSET = -168
        RD_KAFKA_RESP_ERR__OUTDATED = -167
        RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE = -166
        RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE = -165
        RD_KAFKA_RESP_ERR__WAIT_CACHE = -164
        RD_KAFKA_RESP_ERR__INTR = -163
        RD_KAFKA_RESP_ERR__KEY_SERIALIZATION = -162
        RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION = -161
        RD_KAFKA_RESP_ERR__KEY_DESERIALIZATION = -160
        RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION = -159
        RD_KAFKA_RESP_ERR__PARTIAL = -158
        RD_KAFKA_RESP_ERR__READ_ONLY = -157
        RD_KAFKA_RESP_ERR__NOENT = -156
        RD_KAFKA_RESP_ERR__UNDERFLOW = -155
        RD_KAFKA_RESP_ERR__END = -100
        RD_KAFKA_RESP_ERR_UNKNOWN = -1
        RD_KAFKA_RESP_ERR_NO_ERROR = 0
        RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE = 1
        RD_KAFKA_RESP_ERR_INVALID_MSG = 2
        RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART = 3
        RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE = 4
        RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE = 5
        RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION = 6
        RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT = 7
        RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE = 8
        RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE = 9
        RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE = 10
        RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH = 11
        RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE = 12
        RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION = 13
        RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS = 14
        RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE = 15
        RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP = 16
        RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION = 17
        RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE = 18
        RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS = 19
        RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20
        RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS = 21
        RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION = 22
        RD_KAFKA_RESP_ERR_INCONSISTENT_GROUP_PROTOCOL = 23
        RD_KAFKA_RESP_ERR_INVALID_GROUP_ID = 24
        RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID = 25
        RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT = 26
        RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS = 27
        RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE = 28
        RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED = 29
        RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED = 30
        RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED = 31
        RD_KAFKA_RESP_ERR_INVALID_TIMESTAMP = 32
        RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM = 33
        RD_KAFKA_RESP_ERR_ILLEGAL_SASL_STATE = 34
        RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION = 35
        RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS = 36
        RD_KAFKA_RESP_ERR_INVALID_PARTITIONS = 37
        RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR = 38
        RD_KAFKA_RESP_ERR_INVALID_REPLICA_ASSIGNMENT = 39
        RD_KAFKA_RESP_ERR_INVALID_CONFIG = 40
        RD_KAFKA_RESP_ERR_NOT_CONTROLLER = 41
        RD_KAFKA_RESP_ERR_INVALID_REQUEST = 42
        RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT = 43
        RD_KAFKA_RESP_ERR_POLICY_VIOLATION = 44
        RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER = 45
        RD_KAFKA_RESP_ERR_DUPLICATE_SEQUENCE_NUMBER = 46
        RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH = 47
        RD_KAFKA_RESP_ERR_INVALID_TXN_STATE = 48
        RD_KAFKA_RESP_ERR_INVALID_PRODUCER_ID_MAPPING = 49
        RD_KAFKA_RESP_ERR_INVALID_TRANSACTION_TIMEOUT = 50
        RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS = 51
        RD_KAFKA_RESP_ERR_TRANSACTION_COORDINATOR_FENCED = 52
        RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53
        RD_KAFKA_RESP_ERR_SECURITY_DISABLED = 54
        RD_KAFKA_RESP_ERR_OPERATION_NOT_ATTEMPTED = 55

    ctypedef struct rd_kafka_message_t:
        rd_kafka_resp_err_t err
        rd_kafka_topic_t *rkt
        int32_t partition
        void   *payload
        size_t  len
        void   *key
        size_t  key_len
        int64_t offset
        void  *_private

    rd_kafka_t *rd_kafka_new(

            rd_kafka_type_t type,
            rd_kafka_conf_t *conf,
            char *errstr,
            size_t errstr_size
    )

    rd_kafka_topic_t *rd_kafka_topic_new(
            rd_kafka_t *rk,
            const char *topic,
            rd_kafka_topic_conf_t *conf
    );

    const char *rd_kafka_topic_name(const rd_kafka_topic_t *rkt)

    int rd_kafka_produce(
            rd_kafka_topic_t *rkt, int32_t partition,
            int msgflags,
            void *payload, size_t len,
            const void *key, size_t keylen,
            void *msg_opaque
    );

    cdef int _RD_KAFKA_MSG_F_FREE "RD_KAFKA_MSG_F_FREE"
    cdef int _RD_KAFKA_MSG_F_COPY "RD_KAFKA_MSG_F_COPY"
    cdef int _RD_KAFKA_MSG_F_BLOCK "RD_KAFKA_MSG_F_BLOCK"

    cdef int _RD_KAFKA_PARTITION_UA "RD_KAFKA_PARTITION_UA"

    rd_kafka_resp_err_t rd_kafka_assign(
            rd_kafka_t *rk,
            const rd_kafka_topic_partition_list_t *partitions
    )

    ctypedef struct rd_kafka_topic_partition_list_t:
        int cnt
        int size
        rd_kafka_topic_partition_t *elems

    ctypedef struct rd_kafka_topic_partition_t:
        char        *topic
        int32_t      partition
        int64_t        offset
        void        *metadata
        size_t       metadata_size
        void        *opaque
        rd_kafka_resp_err_t err
        void       *_private

    const char *rd_kafka_err2str(rd_kafka_resp_err_t err)

    const char *rd_kafka_message_errstr(const rd_kafka_message_t *rkmessage)

    rd_kafka_topic_conf_t *rd_kafka_topic_conf_new()

    rd_kafka_conf_res_t rd_kafka_topic_conf_set(
            rd_kafka_topic_conf_t *conf,
			const char *name,
			const char *value,
			char *errstr,
            size_t errstr_size
    )

    ctypedef enum rd_kafka_conf_res_t:
        RD_KAFKA_CONF_UNKNOWN = -2
        RD_KAFKA_CONF_INVALID = -1
        RD_KAFKA_CONF_OK = 0

    void rd_kafka_conf_set_rebalance_cb (
            rd_kafka_conf_t *conf,
            void (*rebalance_cb) (
                    rd_kafka_t *rk,
                    rd_kafka_resp_err_t err,
                    rd_kafka_topic_partition_list_t *partitions,
                    void *opaque
            )
    )

    void rd_kafka_conf_set_default_topic_conf (
            rd_kafka_conf_t *conf,
            rd_kafka_topic_conf_t *tconf
    )

    int rd_kafka_brokers_add(rd_kafka_t *rk, const char *brokerlist)

    rd_kafka_resp_err_t rd_kafka_poll_set_consumer (rd_kafka_t *rk)

    rd_kafka_topic_partition_list_t *rd_kafka_topic_partition_list_new(
            int size)
    rd_kafka_topic_partition_t *rd_kafka_topic_partition_list_add(
            rd_kafka_topic_partition_list_t *rktparlist,
            const char *topic, int32_t partition
    )

    rd_kafka_message_t *rd_kafka_consumer_poll(rd_kafka_t *rk, int timeout_ms)
    int rd_kafka_poll(rd_kafka_t *rk, int timeout_ms)

    void rd_kafka_message_destroy(rd_kafka_message_t *rkmessage)
    void rd_kafka_conf_destroy(rd_kafka_conf_t *conf)
    void rd_kafka_topic_conf_destroy(rd_kafka_topic_conf_t *topic_conf)
    void rd_kafka_topic_partition_destroy (rd_kafka_topic_partition_t *rktpar)
    void rd_kafka_topic_partition_list_destroy (
            rd_kafka_topic_partition_list_t *rkparlist)
    void rd_kafka_destroy(rd_kafka_t *rk)
    void rd_kafka_topic_destroy(rd_kafka_topic_t *rkt)
    rd_kafka_resp_err_t rd_kafka_consumer_close (rd_kafka_t *rk)
    int rd_kafka_consume_stop(rd_kafka_topic_t *rkt, int32_t partition)


    rd_kafka_resp_err_t rd_kafka_subscribe (
            rd_kafka_t *rk,
            const rd_kafka_topic_partition_list_t *topics
    )

    const char *rd_kafka_version_str ()

    rd_kafka_resp_err_t rd_kafka_last_error ()
    rd_kafka_resp_err_t rd_kafka_flush (rd_kafka_t *rk, int timeout_ms)

    const char *rd_kafka_name(const rd_kafka_t *rk)

    int64_t rd_kafka_message_timestamp (
            const rd_kafka_message_t *rkmessage,
            rd_kafka_timestamp_type_t *tstype
    )

    ctypedef enum rd_kafka_timestamp_type_t:
            RD_KAFKA_TIMESTAMP_NOT_AVAILABLE,
            RD_KAFKA_TIMESTAMP_CREATE_TIME,
            RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME

    rd_kafka_resp_err_t rd_kafka_assignment(
            rd_kafka_t *rk,
            rd_kafka_topic_partition_list_t **partitions
    )

    rd_kafka_resp_err_t rd_kafka_position(
            rd_kafka_t *rk,
		        rd_kafka_topic_partition_list_t *partitions
    )

    rd_kafka_resp_err_t rd_kafka_subscription(
            rd_kafka_t *rk,
            rd_kafka_topic_partition_list_t **topics
    )

    rd_kafka_resp_err_t rd_kafka_seek(
            rd_kafka_topic_t *rkt,
            int32_t partition,
            int64_t offset,
            int timeout_ms
    )

    rd_kafka_resp_err_t rd_kafka_commit(
            rd_kafka_t *rk,
            const rd_kafka_topic_partition_list_t *offsets,
            int async
    )

    rd_kafka_resp_err_t rd_kafka_offset_store(
            rd_kafka_topic_t *rkt,
			      int32_t partition,
            int64_t offset
    )
