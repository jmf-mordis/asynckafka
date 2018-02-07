import asyncio
import logging


logger = logging.getLogger("asynckafka")


class ConsumerAsyncIterator:
    """
    FIXME the aiter is implemented in a python file because at 2018-02
    cython 0.27 has a bug that prevents compile cython code with
    asyncronous iterators. This bug should be fixed in the 0.28 version.
    """

    def __init__(self, inter_thread_list, get_message, destroy_message, loop):
        """
        :param inter_thread_list: list used for communicate the consumption
        thread with the asyncio thread. The pop and append operations in a
        list are thread safe because them are a single bytecode operation
        in python and the GIL can't switch the thread in the middle of a
        bytecode operation.
        :param get_message: cython function that can reads
        the c structure of the message from the memory address and return
        the payload of the messages in bytes.
        destroy
        :param destroy_message: cython function that tell to
        rdkafka to destroy the memory allocation of the message.
        """
        self.inter_thread_list = inter_thread_list
        self.get_message = get_message
        self.destroy_message = destroy_message
        self._stop = False
        self.loop = loop

    async def get_aiter(self):
        while not self._stop:
            try:
                try:
                    message_memory_address = self.inter_thread_list.pop()
                except IndexError:
                    await asyncio.sleep(0.01, loop=self.loop)
                else:
                    message = self.get_message(message_memory_address)
                    message_memory_view = memoryview(message)
                    self.destroy_message(message_memory_address)
                    yield message_memory_view
            except Exception:
                error_str = "Unexpected exception consuming messages from " \
                            "thread in async iterator consumer"
                logger.error(error_str, exc_info=True)
                raise StopAsyncIteration(error_str)
        raise StopAsyncIteration

    def stop(self):
        self._stop = True

