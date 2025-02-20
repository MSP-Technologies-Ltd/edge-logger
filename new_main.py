import aio_pika
import csv
import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import json
import aioredis

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
REDIS_URL = os.getenv("REDIS_URL")

MAX_ITEMS_PER_DEVICE = 5

redis_client = None

EXCHANGE_NAME = "device"
LOGGING_ROUTING_KEYS = "#.status.#"
REDIS_ROUTING_KEYS = "#.commands.#"

csv_queue = asyncio.Queue()
redis_queue = asyncio.Queue()
latest_messages = {}


async def init_redis():
    global redis_client
    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
    print("Connected to Redis.")


async def save_to_redis(deviceId, data):
    if not redis_client:
        print("Not connected to Redis Instance")
        return

    device_type = deviceId.lower()
    key = f"logs:{device_type}"

    await redis_client.rpush(key, json.dumps(data))
    await redis_client.ltrim(key, -MAX_ITEMS_PER_DEVICE, -1)


async def declare_and_bind_redis(channel: aio_pika.Channel):
    queue_name = "command_logging"
    queue = await channel.declare_queue(queue_name, durable=True)
    await queue.bind(EXCHANGE_NAME, routing_key=REDIS_ROUTING_KEYS)
    print(
        f"Command Logging Queue bound to exchange {EXCHANGE_NAME} with routing key {REDIS_ROUTING_KEYS}"
    )
    return queue


async def consume_and_store_redis(channel: aio_pika.Channel):
    queue = await declare_and_bind_redis(channel)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    pass
                    # body = message.body.decode()
                    # data = json.loads(body)

                    # device_id = data.get("deviceId") or data.get("client_id")
                    # if device_id:
                    #     await save_to_redis(device_id, data)
                except Exception as e:
                    print(f"Error processing Redis message: {e}")


async def declare_and_bind_logging(channel: aio_pika.Channel):
    queue_name = "logging"
    queue = await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-max-length": 5,
            "x-message-ttl": 5000,
            "x-consumer-timeout-action": "ack",
            "x-overflow": "drop-head",
        },
    )
    await queue.bind(EXCHANGE_NAME, routing_key=LOGGING_ROUTING_KEYS)
    print(
        f"Logging Queue bound to exchange {EXCHANGE_NAME} with routing key {LOGGING_ROUTING_KEYS}"
    )
    return queue


async def consume_logging(channel: aio_pika.Channel):
    queue = await declare_and_bind_logging(channel)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    headers = dict(message.headers)

                    body = message.body.decode()
                    data = dict(json.loads(body))

                    timestamp = None
                    device_id = None

                    if headers:
                        # print(headers)

                        if "timestamp" or "Timestamp" in headers:
                            timestamp = headers.get("timestamp") or headers.get(
                                "timestamp"
                            )

                        if "deviceId" in headers:
                            device_id = headers.get("deviceId")

                    if not headers:
                        if "deviceId" or "clientId" in data:
                            device_id = data.get("deviceId") or data.get("clientId")
                    # device_id = data.get("deviceId") or data.get("clientId")

                    # message = data['message']
                    if "message" in data:
                        new_data = None
                        message_data = data["message"]

                        # deal with accuvim
                        if "event" in data:
                            new_data = data["body"]["message"]
                            new_data.pop("rawValues", None)
                            timestamp = data["body"]["timestamp"]
                            device_id = data["headers"]["deviceId"]

                        if timestamp is not None:
                            timestamp_data = {"timestamp": timestamp}
                            new_data = timestamp_data.update(message_data)

                        else:
                            new_data = message_data
                        # new_data = data["message"]

                        if device_id is not None:
                            latest_messages[device_id] = (
                                new_data  # Store only the latest message
                            )

                except Exception as e:
                    print(f"Error processing logging message: {e}")


async def save_csv_file(keys, data, device_id):
    now = datetime.now()
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    documents_path = Path.home() / "Documents" / "Logs" / device_id
    documents_path.mkdir(parents=True, exist_ok=True)

    target_file = (
        documents_path / f"log_{device_id}_{current_hour.strftime('%Y-%m-%d_%H')}.csv"
    )

    mode = "a" if target_file.exists() else "w"
    with target_file.open(mode=mode, newline="") as doc:
        writer = csv.DictWriter(doc, fieldnames=keys)
        if mode == "w":
            writer.writeheader()
        writer.writerow(data)


async def save_messages():
    while True:
        now = datetime.now()

        if latest_messages:
            for device_id, data in latest_messages.items():
                # timestamp = data.get("timestamp")

                keys = list(data.keys())

                # print(device_id, keys)

                if "event" in keys:
                    pass
                if "logged_action" not in keys:
                    if "rawValues" in keys:
                        raw_values = data["rawValues"]
                        # print(raw_values)
                        raw_keys = list(raw_values.keys())

                        new_data = data.pop("rawValues", None)

                        print(f"Saving log for {device_id}")
                        await save_csv_file(keys, new_data, device_id)
                        await save_csv_file(raw_keys, raw_values, "meterRawValues")
                else:
                    print(f"Saving log for {device_id}")
                    await save_csv_file(keys, data, device_id)

                # print(keys)
                # keys.insert(0, "timestamp")

                # for key in keys:
                #     rel_data[key] = data[key]

                # d = {'timestamp': timestamp, **rel_data}

                # print(keys)

        await asyncio.sleep(2)


async def connect_with_retry(max_retries=5, base_delay=2):
    retries = 0
    while True:
        try:
            print("Attempting to connect to RabbitMQ")
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            print("Connected to RabbitMQ")
            return connection
        except Exception as e:
            retries += 1
            if retries > max_retries:
                print("Max retries exceeded. Exiting program.")
                return None
            wait_time = base_delay * (2 ** (retries - 1))
            print(f"Connection failed ({e}). Retrying in {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)


async def app():
    await init_redis()

    while True:
        try:
            connection = await connect_with_retry()
            if not connection:
                break

            async with connection:
                channel = await connection.channel()

                # Start consumers
                consumer_task = asyncio.create_task(consume_logging(channel))
                writer_task = asyncio.create_task(save_messages())
                redis_consumer_task = asyncio.create_task(
                    consume_and_store_redis(channel)
                )

                await asyncio.gather(consumer_task, writer_task, redis_consumer_task)
        except Exception as e:
            print(f"Error: {e}. Retrying in 5 seconds")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(app())
