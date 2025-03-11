import asyncio
import csv
import os
import json
from datetime import datetime
from typing import Dict, List, Any
import aio_pika
import redis.asyncio as redis


with open("./new/config.json", "r") as file:
    config = json.load(file)

RABBIT_URL = config.get("rabbit")
REDIS_URL = config.get("redis")
LOG_DIR = config.get("log_dir_dev")
EXCHANGE_NAME = config.get("exchange_name")

CSV_WRITE_INT = 2

DEBUG = True


class MessageProcessor:
    def __init__(self):
        self.status_messages = []
        self.status_lock = asyncio.Lock()
        self.redis = None
        self.csv_folder = LOG_DIR
        os.makedirs(self.csv_folder, exist_ok=True)

    def get_dev_id(self, data):
        device_id = None

        device_id = data.get("deviceId")

        if device_id is None:
            device_id = data.get("device_id")

        if device_id is None:
            device_id = data.get("Unique ID")

        if device_id is None:
            device_id = data.get("client_id")

        return device_id

    async def setup(self):
        self.redis = await redis.from_url(REDIS_URL)

    async def process_command_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            body = message.body.decode()
            header = message.headers

            if DEBUG:
                print(
                    f"Received command message: {message.routing_key} - {body[:100]}..."
                )

            try:
                data = body if not header else {**body, **header}

                # timestamp = data.get("timestamp") if "timestamp" in data else datetime.now().isoformat()

                device_id = self.get_dev_id(data)
                self.command_lock = asyncio.Lock()
                self.redis = None
                redis_key = f"logs:{device_id}"

                await self.redis.lpush(redis_key, data)

                await self.redis.ltrim(redis_key, 0, 4)

            except json.JSONDecodeError:
                print(f"failed to parse: {body}")

        async def process_status_message(self, message: aio_pika.IncomingMessage):
            async with message.process():
                header = message.headers
                body = message.body.decode()

                try:
                    data = body if not header else {**body, **header}
                    device_id = self.get_dev_id(data)

                    async with self.status_lock:
                        self.status_messages.append(
                            {"deviceId": device_id, "data": data}
                        )

                except json.JSONDecodeError:
                    print(f"failed to parse {body}")

        async def save_status_csv(self):
            while True:
                await asyncio.sleep(CSV_WRITE_INT)

                async with self.status_lock:
                    messages_to_save = self.status_messages.copy()
                    self.status_messages = []

                if not messages_to_save:
                    print("no status msgs")
                    continue

                csv_path = 


async def main():
    processor = MessageProcessor()
    await processor.setup()

    rabbit_conn = await aio_pika.connect_robust(RABBIT_URL)

    async with rabbit_conn:
        channel = await rabbit_conn.channel()

        exchange = await channel.get_exchange(EXCHANGE_NAME)

        commands_queue = await channel.declare_queue(
            "command_logging",
            # auto_delete=True,
            arguments={
                "x-max-length": 30,
                "x-message-ttl": 5000,
                "x-consumer-timeout-action": "ack",
                "x-overflow": "drop-head",
            },
        )

        await commands_queue.bind(exchange, routing_key="#.commands.#")
        await commands_queue.bind(exchange, routing_key="#_commands_#")

        status_queue = await channel.declare_queue(
            "logging",
            # auto_delete=True,
            arguments={
                "x-max-length": 30,
                "x-message-ttl": 5000,
                "x-consumer-timeout-action": "ack",
                "x-overflow": "drop-head",
            },
        )

        await status_queue.bind(exchange, routing_key="#.status.#")

        await commands_queue.consume(processor.process_command_message)

        try:
            await asyncio.Future()

        except KeyboardInterrupt:
            exit


if __name__ == "__main__":
    asyncio.run(main())
