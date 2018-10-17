import asyncio
import msgpack
import uuid
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

loop = asyncio.get_event_loop()

kafka_host = None
producer = None

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

async def init(host):
    global kafka_host
    global producer

    kafka_host = host
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers=kafka_host)
    await producer.start()

async def subscribe(topic, callback):
    consumer = AIOKafkaConsumer(
        topic,
        loop=loop, bootstrap_servers=kafka_host)
    await consumer.start()

    async for msg in consumer:
        try:
            value = msgpack.unpackb(msg.value, raw=False)
            callback({ "key": decode_key(msg.key), "value": value })
        except:
            print("Not binary data")

    # await consumer.stop()

async def push(topic, value, key=None):
    data = msgpack.packb(value, use_bin_type=True)
    await producer.send(topic, data, key=encode_key(key))

async def fetch(to, _from, value):
    print("Hello fetch")
    id = str(uuid.uuid4())

    consumer = AIOKafkaConsumer(
        _from,
        loop=loop, bootstrap_servers=kafka_host)

    await push(to, value, id)
    await consumer.start()

    async for msg in consumer:
        try:
            key = decode_key(msg.key)
            if key == id:
                await consumer.stop()
                return msgpack.unpackb(msg.value, raw=False)
        except:
            print("Not binary data")
