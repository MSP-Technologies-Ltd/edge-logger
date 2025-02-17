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
import aiohttp

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
RABBITMQ_API = os.getenv("RABBITMQ_API_URL")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")

DEVICE_NAMES = ["inverter", "meter", "battery", "controller"]


exchange_name = "device"

QUEUE_REGEX = re.compile("(inverter|meter|battery|controller)_status_queue_(\d+)")

message_queues = defaultdict(Queue)
active_consumers = {}


async def discover_queues():
    auth = aiohttp.BasicAuth(RABBITMQ_USER, RABBITMQ_PASS)
    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.get(RABBITMQ_API) as response:
            if response.status == 200:
                data = await response.json()
                return [q["name"] for q in data]
            else:
                return []


async def monitor_queues():
    while True:
        queues = await discover_queues()

        for queue_name in queues:
            match = re.match(QUEUE_REGEX, queue_name)
            if match:
                device_id = match.group(1)

                if queue_name not in active_consumers:
                    task = asyncio.create_task(consume_queue(queue_name, device_id))
                    active_consumers[queue_name] = task

        await asyncio.sleep(5)


async def consume_queue(queue_name, device):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = message.body.decode()
                    print(f"Received from {queue_name}: {data}")
                    message_queues[device].put_nowait(data)


async def create_log_file(device, inputData):
    while True:
        log_data = dict()

        now = datetime.now()

        current_hour = now.replace(minute=0, second=0, microsecond=0)

        documents_path = Path.home() / "Documents" / "Logs"

        documents_path.mkdir(parents=True, exist_ok=True)

        target_file = (
            documents_path / f"log_{device}_{current_hour.strftime('%Y-%m-%d_%H')}.csv"
        )

        list_of_dicts = []

        for _ in range(20):
            log_data = inputData
            if log_data:
                list_of_dicts.append(log_data)
            time.sleep(1)

        mode = "a" if target_file.exists() else "w"
        with target_file.open(mode=mode, newline="") as doc:
            writer = csv.DictWriter(doc, fieldnames=list_of_dicts[0].keys())
            if mode == "w":
                writer.writeheader()
            writer.writerows(list_of_dicts)
            print("\r" + f"Write to {target_file}")

        list_of_dicts.clear()

        if datetime.now() >= current_hour + timedelta(hours=1):
            break


async def discover_consume():
    while True:
        timestamp = int(datetime.now().timestamp() * 1000)

        for device in DEVICE_NAMES:
            queue_name = f"{device}_status_queue_{timestamp}"

            if QUEUE_REGEX.match(queue_name):
                if queue_name not in active_consumers:
                    print(f"Starting consumer for queue: {queue_name}")
                    task = asyncio.create_task(consume_queue(queue_name, device))
                    active_consumers[queue_name] = task

        await asyncio.sleep(5)


async def app():
    # lock = asyncio.Lock()

    try:
        consumer_task = asyncio.create_task(discover_consume())

        writer_tasks = [
            asyncio.create_task(create_log_file(device)) for device in DEVICE_NAMES
        ]

        await asyncio.gather(consumer_task, *writer_tasks)

    except KeyboardInterrupt:
        print("Stopping...")

    except Exception as e:
        print(e)


if __name__ == "__main__":
    asyncio.run(app())
