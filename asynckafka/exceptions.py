

class InvalidSetting(Exception):
    pass


class UnknownSetting(Exception):
    pass


class ConsumerError(Exception):
    pass


class ProducerError(Exception):
    pass


class KafkaError(Exception):
    """
    Input of error callback.
    """
    def __init__(self, rk_name, error_code, error_str, reason,
                 consumer_or_producer):
        self._rk_name = rk_name
        self._error_code = error_code
        self._error_str = error_str
        self._reason = reason
        self._consumer_or_producer = consumer_or_producer
        super().__init__(error_str)

    @property
    def rk_name(self):
        """
        Rdkafka name of the producer or consumer that trigger
        the error_callback.

        Returns:
            str: RDK name to identify the consumer or producer.
        """
        return self._rk_name

    @property
    def error_code(self):
        """
        Rdkafka error code.

        Returns:
            int: RdKafka error code.
        """
        return self._error_code

    @property
    def error_str(self):
        """
        Text provided by rdkafka with a human readable description of the
        error.

        Returns:
            str: Text describing the error.
        """
        return self.error_str

    @property
    def reason(self):
        """
        Text provided by rdkafka describing possible causes of the error.

        Returns:
            str: Text describing the reason of the error.
        """
        return self._reason

    @property
    def consumer_or_producer(self):
        """
        Returns the producer or consumer that trigger the error.

        Returns:
            Union[asynckafka.Consumer, asynckafka.Producer]: Consumer or
            producer object that triggered the error.
        """
        return self._consumer_or_producer

    def is_from_consumer(self):
        """
        Check if the error is triggered by a consumer.

        Returns:
            bool: True if it is a consumer else if not
        """
        return type(self._consumer_or_producer).__name__ == 'Consumer'

    def is_from_producer(self):
        """
        Check if the error is triggered by a producer.

        Returns:
            bool: True if it is a producer else if not
        """
        return type(self._consumer_or_producer).__name__ == 'Producer'

    def __repr__(self):
        return "Kafka error in {rk_name}. " \
               "Error code {error_code}. " \
               "{error_str} {reason}".format(
                    rk_name=self._rk_name, error_code=self._error_code,
                    error_str=self._error_str, reason=self._reason
                )
