import asyncio
from .__main__ import subscribe, push
from types import CoroutineType

def receive_action(producer: str, actions: dict):
    async def process_action(action):
        if (action == None or action.get('value') == None or
            action['value'].get('action') == None):
            return

        func = actions.get(action['value']['action'])

        if func == None:
            return

        try:
            result = func(action['value'].get('data'))

            if type(result) is CoroutineType:
                result = await result

            if result == None:
                return

            if not type(result) is Responce:
                result = { 'data': result }
        except Exception as e:
            result = { 'error': str(e) }

        await push(producer, result, action.get('key'))

    return process_action

class Listener(object):
    def __init__(self, consumer: str, producer: str, actions: dict):
        asyncio.ensure_future(subscribe(
            consumer,
            receive_action(producer, actions)
        ))

class Responce(dict):
    pass
