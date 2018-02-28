API
===


.. automodule:: asynckafka

.. automethod:: set_debug
.. automethod:: is_in_debug
.. automethod:: set_error_callback


Consumer
--------
.. autoclass:: asynckafka.Consumer
.. automethod:: asynckafka.Consumer.__init__
.. automethod:: asynckafka.Consumer.start
.. automethod:: asynckafka.Consumer.stop
.. automethod:: asynckafka.Consumer.is_consuming
.. automethod:: asynckafka.Consumer.is_stopped

Producer
--------
.. autoclass:: asynckafka.Producer
.. automethod:: asynckafka.Producer.__init__
.. automethod:: asynckafka.Producer.produce
.. automethod:: asynckafka.Producer.start
.. automethod:: asynckafka.Producer.stop
.. automethod:: asynckafka.Producer.is_started
