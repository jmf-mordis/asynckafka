
class InvalidSetting(Exception):
    pass


class UnknownSetting(Exception):
    pass


class InvalidBrokers(Exception):
    pass


class SubscriptionError(Exception):
    pass


class ConsumerError(Exception):
    pass


class ProducerError(Exception):
    pass


class MessageError(Exception):
    pass


class KafkaError(Exception):

    def __init__(self, rk_name, error_code, error_str, reason):
        self.rk_name = rk_name
        self.error_code = error_code
        self.error_str = error_str
        self.reason = reason
        super().__init__(error_str)
