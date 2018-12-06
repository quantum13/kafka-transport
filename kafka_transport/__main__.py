import asyncio
import logging

import msgpack
import uuid
import time
import atexit
from types import CoroutineType
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = logging.getLogger('kafka_transport')

loop = asyncio.get_event_loop()

kafka_host = None
producer = None

class KafkaTransportError(Exception):
    def __init__(self, msg):
        self.msg = msg
    
    def __str__(self):
        return self.msg


def encode_key(key) -> str:
    if key == None:
        return None

    if type(key) is int:
        key = str(key)

    return key.encode('utf8')


def decode_key(key) -> str:
    if key == None:
        return None

    if type(key) is int:
        return key

    return key.decode('utf8')


async def init(host, loop=loop):
    global kafka_host
    global producer

    kafka_host = host

    if not producer is None:
        close()

    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers=kafka_host)
    await producer.start()


def close():
    if producer is not None:
        asyncio.ensure_future(producer.stop())
atexit.register(close)


async def subscribe(topic, callback, loop=loop, consumer_options=None):
    consumer = AIOKafkaConsumer(
        topic,
        loop=loop, bootstrap_servers=kafka_host,
        **(consumer_options if type(consumer_options) is dict else {})
    )
    await consumer.start()

    async for msg in consumer:
        try:
            value = msgpack.unpackb(msg.value, raw=False)
        except:
            logger.warning("Not binary data: %s", str(msg.value))
            continue

        try:
            result = callback({
                "key": decode_key(msg.key),
                "value": value
            })
            if type(result) is CoroutineType:
                asyncio.ensure_future(result)
        except:
            logger.warning("Error during calling handler with data: %s", str(value))


async def push(topic, value, key=None):
    data = msgpack.packb(value, use_bin_type=True)
    await producer.send_and_wait(topic, data, key=encode_key(key))


async def fetch(to, _from, value, timeout_ms=600 * 1000, loop=loop):
    id = str(uuid.uuid4())

    consumer = AIOKafkaConsumer(
        _from,
        loop=loop, bootstrap_servers=kafka_host)

    await consumer.start()
    await asyncio.sleep(0.5)
    
    await push(to, value, id)

    try:
        end_time = time.time() + timeout_ms / 1000

        while time.time() <= end_time:
          result = await consumer.getmany(timeout_ms=timeout_ms)
          for messages in result.values():
              for msg in messages:
                  key = decode_key(msg.key)
                  if key == id:
                      await consumer.stop()
                      return msgpack.unpackb(msg.value, raw=False)
    finally:
        await consumer.stop()

    raise KafkaTransportError("Fetch timeout")

