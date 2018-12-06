import asyncio
import logging

from .__main__ import subscribe, push
from types import CoroutineType

logger = logging.getLogger('kafka_transport')


def produce_error():
    async def _produce_error(msg, exception, consumer_topic: str, producer_topic: str):
        await push(
            producer_topic,
            {
                'error': str(exception)
            },
            msg['key']
        )
    return _produce_error


def resend_message(reshipments_count=10):
    async def _resend_message(msg, exception, consumer_topic: str, producer_topic: str):
        reshipment_num = msg.get('value').get('reshipment_num', 0)
        if reshipment_num >= reshipments_count:
            logger.error("Stopping resending message: %s", str(msg))
        else:
            logger.error("Resending message: %s", str(msg))
            await push(
                consumer_topic,
                {
                    **msg['value'],
                    'reshipment_num': reshipment_num+1
                },
                msg['key']
            )

    return _resend_message


def receive_action(consumer_topic: str, producer_topic: str, actions: dict, on_error):
    async def process_action(msg):
        if type(msg) is not dict or type(msg.get('value')) is not dict or \
                not msg['value'].get('action'):
            return

        key = msg.get('key')
        value = msg.get('value')

        func = actions.get(value['action'])

        if func is None:
            return

        try:
            result = func(value.get('data'))

            if type(result) is CoroutineType:
                result = await result

            if result is None:
                return

            if type(result) is not Response:
                result = { 'data': result }
        except Exception as e:
            logger.error("Error during processing message: %s (%s)", str(msg), str(e))
            await on_error(msg, e, consumer_topic, producer_topic)
            return

        await push(producer_topic, result, key)

    return process_action


class Listener(object):
    def __init__(self,
                 consumer_topic: str, producer_topic: str,
                 actions: dict, on_error=produce_error(),
                 consumer_options=None):
        assert type(actions) is dict, 'actions must be dict'

        asyncio.ensure_future(subscribe(
            consumer_topic,
            receive_action(consumer_topic, producer_topic, actions, on_error),
            consumer_options=consumer_options
        ))


class Response(dict):
    pass
