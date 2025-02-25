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
    queue = await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-max-length": 30,
            "x-message-ttl": 5000,
            "x-consumer-timeout-action": "ack",
            "x-overflow": "drop-head",
        },
    )
    await queue.bind(EXCHANGE_NAME, routing_key=REDIS_ROUTING_KEYS)
    print(
        f"Command Logging Queue bound to exchange {EXCHANGE_NAME} with routing key {REDIS_ROUTING_KEYS}"
    )
    return queue


"""Not working as expected and also not necessary at this point"""
# def match_command_structure(obj):
#     if isinstance(obj, dict):
#         expected_keys = {
#             "deviceType": str,
#             "deviceId": str,
#             "commandSeq": str,
#             "loggedAction": str,
#             "timestamp": str or datetime,
#         }
#         for key, expected_type in expected_keys.items():
#             if key not in obj or not isinstance(obj[key], expected_type):
#                 return False
#             return True
#         return False


# def match_status_structure(obj):
#     if isinstance(obj, dict):
#         expected_keys = {
#             "deviceType": str,
#             "deviceId": str,
#             "from": str,
#             "to": str,
#             "commandSeq": str,
#             "commandHint": str,
#             "message": dict,
#             "action": str,
#             "timestamp": str or datetime,
#         }

#     for key, expected_type in expected_keys.items():
#         if key not in obj or not isinstance(obj[key], expected_type):
#             return False
#         return True
#     return False


async def consume_and_store_redis(channel: aio_pika.Channel):
    queue = await declare_and_bind_redis(channel)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    body = message.body.decode()
                    data = json.loads(body)

                    # if match_command_structure(data) is True:
                    device_id = data.get("deviceId")
                    if device_id is not None:
                        await save_to_redis(device_id, data)

                    # else:
                    #     print("does not match expected message format")

                except Exception as e:
                    print(
                        f"Error processing Redis message: {e}. Message received: {data}"
                    )


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
        # print("async with queue iter in consume_logging")
        async for message in queue_iter:
            # print("async for message in queue iter")
            async with message.process():
                try:
                    body = message.body.decode()

                    data = json.loads(body)

                    # if match_status_structure(data):
                    device_id = data.get("deviceId")

                    if device_id is None:
                        headers = message.headers

                        # print(headers)

                        timestamp = headers.get("timestamp")
                        device_id = headers.get("device_id")

                        if timestamp is None:
                            timestamp = datetime.now().isoformat()

                        if device_id is None:
                            device_id = headers.get("Unique ID")

                        message_data = data.get("message")

                        device_info = {
                            "deviceId": device_id,
                            "timestamp": str(timestamp),
                        }

                        if message_data is not None:
                            if "standard" in message_data:

                                standard_data = message_data.get("standard")
                                parsed_data = message_data.get("data")

                                standard_data = {**device_info, **standard_data}
                                parsed_data = {**device_info, **parsed_data}

                                latest_messages[f"{device_id} - standard"] = (
                                    standard_data
                                )
                                parsed_data[f"{device_id} - parsed"] = parsed_data
                        else:
                            print("I reckon this is an inverter")
                    else:
                        latest_messages[device_id] = data

                    # timestamp = data.get("timestamp")

                    # device_info = {
                    #     "deviceId": device_id,
                    #     "timestamp": str(timestamp),
                    # }

                    # status_data = data.get("status")

                    # if status_data is not None:
                    # all_data = {**device_info, **data}

                    # latest_messages[device_id] = all_data

                except Exception as e:
                    print(f"Error processing logging message: {e}, message: {data}")


async def save_csv_file(keys, data, device_id):
    now = datetime.now()
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    log_dir = "/app/Logs"
    # documents_path = Path.home() / "Documents" / "Logs" / f"{device_id}"
    documents_path = Path(log_dir) / f"{device_id}"
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
        if latest_messages:
            for device_id, data in latest_messages.items():

                keys = list(data.keys())

                await save_csv_file(keys, data, device_id)
                # print(f"Saving log for {device_id}")

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

                consumer_task = asyncio.create_task(consume_logging(channel))
                writer_task = asyncio.create_task(save_messages())
                redis_consumer_task = asyncio.create_task(
                    consume_and_store_redis(channel)
                )

                await asyncio.gather(consumer_task, writer_task, redis_consumer_task)
        except Exception as e:
            print(f"Error: {e}. Retrying.")
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(app())
