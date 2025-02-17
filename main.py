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

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
RABBITMQ_API = os.getenv("RABBITMQ_API_URL")

EXCHANGE_NAME = "device"
LOGGING_ROUTING_KEYS = "#.status.#"

DEVICE_NAMES = ["inverter", "meter", "battery", "controller"]

QUEUE_REGEX = re.compile(
    r"(?P<device>inverter|meter|battery|controller)_status_queue_(?P<timestamp>\d+)"
)

message_queue = asyncio.Queue()
active_consumers = {}


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
            async with message.process():
                headers = message.headers
                body = message.body.decode()

                data = dict(json.loads(body))

                data = data["message"]

                print(data)

                # data = body["message"]

                # print(data)

                # if headers:
                #     deviceId = headers.get("deviceId")
                #     await message_queue.put((deviceId, body))

                # await message_queue.put((device_id, message_json))
                await asyncio.sleep(2)


async def save_messages():
    while True:
        now = datetime.now()
        current_hour = now.replace(minute=0, second=0, microsecond=0)

        documents_path = Path.home() / "Documents" / "Logs"

        documents_path.mkdir(parents=True, exist_ok=True)

        messages = []

        while not message_queue.empty():
            messages.append(await message_queue.get())

        if messages:

            target_file = (
                documents_path
                / f"log_{messages[0][0]}_{current_hour.strftime('%Y-%m-%d_%H')}.csv"
            )

            # print(messages)
            await asyncio.to_thread(write_to_file, target_file, messages)

        await asyncio.sleep(2)


def write_to_file(filename, messages):
    file_exists = os.path.exists(filename)

    mode = "a" if file_exists else "w"

    with open(filename, mode, newline="") as f:
        for message in messages:

            print(message)
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


async def app():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        consumer_task = asyncio.create_task(consume_logging(channel))

        writer_task = asyncio.create_task(save_messages())

        await asyncio.gather(consumer_task, writer_task)


if __name__ == "__main__":
    try:
        asyncio.run(app())
    except Exception as e:
        print(e)

# # writes to csv file with a given file name/location
# def write_file(filename, data):
#     file_exists = os.path.exists(filename)

#     with open(filename, mode="a", newline="") as f:
#         writer = csv.writer(f)

#         if not file_exists:
#             writer.writerow(["timestamp", "message"])

#         for message in data:
#             writer.writerow(
#                 [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message]
#             )


# # saves csv file
# async def save_to_csv(device_id):
#     while True:
#         now = datetime.now()

#         current_hour = now.replace(minute=0, second=0, microsecond=0)

#         documents_path = Path.home() / "Documents" / "Logs"

#         documents_path.mkdir(parents=True, exist_ok=True)

#         target_file = (
#             documents_path
#             / f"log_{device_id}_{current_hour.strftime('%Y-%m-%d_%H')}.csv"
#         )

#         messages = []

#         while not message_queues[device_id].empty():
#             messages.append(await message_queues[device_id].get())

#         if messages:
#             await asyncio.to_thread(write_file, target_file, messages)

#         await asyncio.sleep(2)
