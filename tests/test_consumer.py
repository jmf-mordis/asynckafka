import pytest

from asynckafka import Consumer


async def example_message_handler(message: bytes):
    print(message.decode('utf-8'))


@pytest.mark.parametrize("dict_input, expected_output", [
    ({'_': 'value'}, {'.': 'value'})
])
def test_consumer_init_settings(dict_input, expected_output):
    dict_output = Consumer._parse_settings(dict_input)
    assert dict_output == expected_output


@pytest.mark.asyncio
async def test_consumer_instance(event_loop):
    Consumer(
        brokers="127.0.0.1:9092", topic='my_topic',
        message_handler=example_message_handler,
        loop=event_loop, consumer_settings={'group.id': 'my_consumer_group'}
    )