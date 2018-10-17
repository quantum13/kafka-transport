import asyncio
import msgpack
import uuid
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

loop = asyncio.get_event_loop()

kafka_host = None
producer = None

async def init(host):
    print("Hello init")
    global kafka_host
    global producer

    kafka_host = host
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers=kafka_host)
    await producer.start()

async def subscribe(topic, callback):
    print("Hello subscribe")
    consumer = AIOKafkaConsumer(
        topic,
        loop=loop, bootstrap_servers=kafka_host)
    await consumer.start()

    async for msg in consumer:
        try:
            value = msgpack.unpackb(msg.value, raw=False)
            callback({ "key": msg.key.decode('utf8'), "value": value })
        except:
            print("Not binary data")

    # await consumer.stop()

async def push(topic, value, key):
    print("Hello push")
    data = msgpack.packb(value, use_bin_type=True)
    await producer.send(topic, data, key=key.encode('utf8'))

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
            key = msg.key.decode('utf8')
            if key == id:
                await consumer.stop()
                return msgpack.unpackb(msg.value, raw=False)
        except:
            print("Not binary data")
