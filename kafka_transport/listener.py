import asyncio
import logging
import uuid

from .__main__ import subscribe, push
from types import CoroutineType

logger = logging.getLogger('kafka_transport')


def produce_error():
    async def _produce_error(msg, exception, consumer_topic: str, producer_topic: str):
        await push(
            producer_topic,
            {
                'error': [str(exception)]
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


class Listener(object):
    def __init__(self,
                 consumer_topic: str, producer_topic: str,
                 consumer_options=None):
        self.actions = {}
        self.actions_on_error = {}
        self.msg_to_wait = set()
        self.msg_to_wait_results = {}
        self.consumer_topic = consumer_topic
        self.producer_topic = producer_topic

        asyncio.ensure_future(
            subscribe(
                consumer_topic,
                self.process_msg,
                consumer_options=consumer_options
            )
        )

    async def process_msg(self, msg):
        if self.msg_to_wait:
            self._process_msg_to_wait(msg)

        if self.actions:
            await self._process_action(msg)

    async def fetch(self, data):
        key = str(uuid.uuid4())
        self.msg_to_wait.add(key)
        await push(self.producer_topic, data, key)

        while key not in self.msg_to_wait_results:
            await asyncio.sleep(0.01)
        result = self.msg_to_wait_results[key]
        del self.msg_to_wait_results[key]

        return result

    def add_actions(self, actions: dict, on_error=produce_error()):
        assert type(actions) is dict, 'Actions must be dict'
        assert not set(self.actions.keys()) & set(actions.keys()), "Actions already added"

        self.actions = {**self.actions, **actions}
        for action_name in actions.keys():
            self.actions_on_error[action_name] = on_error

    def _process_msg_to_wait(self, msg):
        if msg.get('key') in self.msg_to_wait:
            self.msg_to_wait_results[msg.get('key')] = msg.get('value')
            self.msg_to_wait -= {msg.get('key')}

    async def _process_action(self, msg):
        if type(msg) is not dict or type(msg.get('value')) is not dict or \
                not msg['value'].get('action'):
            return

        key = msg.get('key')
        value = msg.get('value')

        func = self.actions.get(value['action'])

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
            on_error = self.actions_on_error.get(value['action'])
            if on_error:
                await on_error(msg, e, self.consumer_topic, self.producer_topic)
            return

        await push(self.producer_topic, result, key)


class Response(dict):
    pass
