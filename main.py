import aio_pika
from aio_pika import ExchangeType
import csv
import os
import asyncio
from asyncio import Queue
from datetime import datetime, timedelta
import time
from collections import defaultdict
from dotenv import load_dotenv
from pathlib import Path
import re
import json
import aioredis

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
# RABBITMQ_API = os.getenv("RABBITMQ_API_URL")

MAX_ITEMS_PER_DEVICE = 5
REDIS_URL = os.getenv("REDIS_URL")
redis_client = None


EXCHANGE_NAME = "device"
LOGGING_ROUTING_KEYS = "#.status.#"
REDIS_ROUTING_KEYS = "#.commands.#"

DEVICE_NAMES = ["inverter", "meter", "battery", "controller"]

QUEUE_REGEX = re.compile(
    r"(?P<device>inverter|meter|battery|controller)_status_queue_(?P<timestamp>\d+)"
)

csv_queue = asyncio.Queue()
redis_queue = asyncio.Queue()
active_consumers = {}


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

    # print(f"Saved {device_type} to Redis")


async def declare_and_bind_redis(channel: aio_pika.channel):
    queue_name = "command_logging"
    queue = await channel.declare_queue(queue_name, durable=True)
    await queue.bind(EXCHANGE_NAME, routing_key=REDIS_ROUTING_KEYS)
    print(f"Command Logging Queue bound to exchange {EXCHANGE_NAME} with routing key {REDIS_ROUTING_KEYS}")
    return queue


async def consume_and_store_redis(channel: aio_pika.channel):
    queue = await declare_and_bind_redis(channel)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    body = message.body.decode()
                    data = dict(json.loads(body))
                    # print(data)

                    device_id = None['logged_action']

                    if data["deviceId"] is not None:
                        device_id = data["deviceId"]
                    else:
                        device_id = data["client_id"]

                    if device_id is not None:
                        await save_to_redis(device_id, data)
                except Exception as e:
                    # print(e)
                    pass


async def declare_and_bind(channel: aio_pika.channel):
    queue_name = "logging"
    queue = await channel.declare_queue(queue_name, durable=True)

    await queue.bind(EXCHANGE_NAME, routing_key=LOGGING_ROUTING_KEYS)

    print(
        f"Logging Queue bound to exchange {EXCHANGE_NAME} with routing key {LOGGING_ROUTING_KEYS}"
    )

    return queue


async def consume_logging(channel: aio_pika.channel):
    queue = await declare_and_bind(channel)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            headers = dict(message.headers)
            print(headers)

            body = message.body.decode()

            data = dict(json.loads(body))

            await csv_queue.put(data)

            await message.ack()

            await asyncio.sleep(2)


async def save_csv_file(keys, data, device_id):
    now = datetime.now()
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    documents_path = Path.home() / "Documents" / "Logs" / f"{device_id}"

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
        messages = []

        while not csv_queue.empty():
            messages.append(await csv_queue.get())

        if messages:
            for message in messages:
                # check for valid message format
                if type(message) is dict:
                    data = message["message"]

                    if "standard" or "data" in data:
                        standard_data = dict(data["standard"])
                        extended_data = dict(data["data"])

                        standard_keys = list(standard_data.keys())
                        extended_keys = list(extended_data.keys())

                        await save_csv_file(standard_keys, standard_data, "battery_standard")
                        await save_csv_file(extended_keys, extended_data, "battery_extended")

                    
                    if "logged_action" in data:
                        logged_action = data['logged_action']

                        if "Battery" in logged_action:
                            if "standard" or "data" in data:
                                standard_data = dict(data["standard"])
                                extended_data = dict(data["data"])

                                standard_keys = list(standard_data.keys())
                                extended_keys = list(extended_data.keys())

                                await save_csv_file(standard_keys, standard_data, "battery_standard")
                                await save_csv_file(extended_keys, extended_data, "battery_extended")

                        elif "Controller" in logged_action:
                            keys = list(data.keys())

                            # print('timestamp' in keys)
                            
                            for key in keys:
                                if key == 'global_state':
                                    keys.remove('global_state')

                                    for i in data['global_state']:
                                        keys.append(i)
                            
                            data_dict = dict()

                            for key in keys:
                                if key in data:
                                    data_dict[key] = data[key]
                                elif key not in data:
                                    data_dict[key] = data['global_state'][key]

                            data_dict_keys = list(data_dict.keys())

                            await save_csv_file(data_dict_keys, data_dict, "controller")

                            
                    else:
                        pass

                    # if "standard" in data:
                    #     # print("this is a battery, continue: ")

                    #     standard_data = dict(data["standard"])
                    #     extended_data = dict(data["data"])

                    #     standard_keys = list(standard_data.keys())
                    #     extended_keys = list(extended_data.keys())

                    #     await save_csv_file(standard_keys, standard_data, "battery")

                    # else:
                    #     print(data.get('logged_action'))

                    #     exit()

                        # print(list(data.keys()))  
                    # print("message type is dictionary")

        # now = datetime.now()
        # current_hour = now.replace(minute=0, second=0, microsecond=0)

        # documents_path = Path.home() / "Documents" / "Logs"

        # documents_path.mkdir(parents=True, exist_ok=True)

        # messages = []

        # while not message_queue.empty():
        #     messages.append(await message_queue.get())

        # if messages:

        #     target_file = (
        #         documents_path
        #         / f"log_{messages[0][0]}_{current_hour.strftime('%Y-%m-%d_%H')}.csv"
        #     )

        #     # print(messages)
        #     await asyncio.to_thread(write_to_file, target_file, messages)

        await asyncio.sleep(2)


"""I think this should be included in the above, but possibly with different methods per device type"""
# def write_to_file(filename, messages):
#     file_exists = os.path.exists(filename)

#     mode = "a" if file_exists else "w"

#     with open(filename, mode, newline="") as f:
#         for message in messages:

#             print(message)
# print(message)
# print(message["message"])
# writer = csv.DictWriter(f, fieldName=message.keys())

# if mode == "w":
#     writer.writeheader()
# writer.writerows(message.value())

# writer = csv.DictWriter(f,)

# if not file_exists:
# writer.writerow(["timestamp", "message"])

# for message in messages:
#     writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message])


async def connect_with_retry(max_retries=5, base_delay=2):
    retries = 0
    while True:
        try:
            print("Attempting to connect to RabbitMQ")
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            print("Connected")
            return connection
        except Exception as e:
            retries += 1
            if retries > max_retries:
                print("Max retries exceeded. Exiting Program.")
            wait_time = base_delay * (2 ** (retries - 1))
            print(f"Connection failed ({e}). Retrying in {wait_time: .2f} seconds")
            await asyncio.sleep(wait_time)


async def app():
    await init_redis()

    while True:
        try:
            connection = await connect_with_retry()
            async with connection:
                channel = await connection.channel()
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
