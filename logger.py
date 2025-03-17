import aio_pika
import csv
import asyncio
from datetime import datetime
from pathlib import Path
import json
import aioredis

with open("./config.json", "r") as file:
    config = json.load(file)

RABBIT_URL = config.get("rabbit", None)
REDIS_URL = config.get("redis", None)
LOG_DIR = config.get("log_dir_dev", None)
EXCHANGE_NAME = config.get("exchange_name", None)
LOGGING_ROUTING_KEYS = config.get("logging_keys", None)
COMMAND_ROUTING_KEYS = config.get("command_keys", None)

redis_client = None
latest_messages = {}


def get_device_id(data):
    device_id = None

    device_id = data.get("deviceId")

    if device_id is None:
        device_id = data.get("device_id")

    if device_id is None:
        device_id = data.get("Unique ID")

    if device_id is None:
        device_id = data.get("client_id")

    return device_id

async def init_redis():
    global redis_client
    try:
        if not REDIS_URL:
            print("Redis URL not available")
            return

        redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)

        try:
            ping_result = await redis_client.ping()

            if ping_result:
                print("Connected to Redis.")
        except Exception as e:
            print(f"Error during Redis ping test: {e}")
            redis_client = None
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        redis_client = None


async def init_rabbit():
    retries = 0
    max_retries = 5
    base_delay = 2
    while True:
        try:
            print("Attempting to connect to RabbitMQ")
            connection = await aio_pika.connect_robust(RABBIT_URL)

            if connection:
                print("Connected to RabbitMQ")
                return connection
        except Exception as e:
            retries += 1
            if retries > max_retries:
                print("Max retries exceeded.")
                return None
            wait_time = base_delay * (2 ** (retries - 1))
            print(f"Connection failed {e}. Retrying in {wait_time:.2f} seconds")
            asyncio.sleep(wait_time)


async def save_csv_file(keys, data, devId, commandMessage):
    now = datetime.now()

    current_hour = now.replace(minute=0, second=0, microsecond=0)

    target_path = (
        (Path(LOG_DIR) / f"{devId}")
        if not commandMessage
        else Path(LOG_DIR) / "commands" / f"{devId}"
    )

    target_path.mkdir(parents=True, exist_ok=True)

    target_file = (
        (target_path / f"log_{devId}_{current_hour.strftime('%Y-%m-%d_%H')}.csv")
        if not commandMessage
        else (
            target_path / f"commands_{devId}_{current_hour.strftime('%Y-%m-%d_%H')}.csv"
        )
    )

    mode = "a" if target_file.exists() else "w"

    with target_file.open(mode=mode, newline="") as doc:
        writer = csv.DictWriter(doc, fieldnames=keys)
        if mode == "w":
            writer.writeheader()
        writer.writerow(data)


async def save_status_messages():
    while True:
        if latest_messages:
            for devId, data in latest_messages.items():
                if data:
                
                    keys = list(data.keys())

                    await save_csv_file(keys, data, devId, commandMessage=False)

        await asyncio.sleep(2)


async def consume_logging_queue(channel: aio_pika.Channel):
    queue = await declare_and_bind_logging(channel)
    try:
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    headers = message.headers
                    body = message.body.decode()
                    
                    data = json.loads(body)
                    
                    full_message = {**headers, **data} if headers else {**data}

                    device_id = get_device_id(full_message)

                    if full_message is not None:
                        if device_id is not None:
                            timestamp = data.get("timestamp", datetime.now().isoformat())

                            if "message" in full_message:
                                batt_data = full_message.get("message", None)
                                standard_data = batt_data.get("standard", None)
                                unparsed_data = batt_data.get("data", None)
                                parsed_data = batt_data.get("parsed data", None)

                                if standard_data is not None:
                                    latest_messages[f"{device_id} - standard data"] = {"timestamp": timestamp, **standard_data}

                                if unparsed_data is not None:
                                    latest_messages[f"{device_id} - unparsed data"] = {"timestamp": timestamp, **unparsed_data}

                                if parsed_data is not None:
                                    latest_messages[f"{device_id} - parsed data"] = {"timestamp": timestamp, **parsed_data}

                            if "client_id" in full_message:
                                latest_messages[f"{device_id}"] = {"timestamp": timestamp, **full_message}
    except json.JSONDecodeError as e:
        print(f"aaaaa, {e}")
    except Exception as e:
        print(f"Error in consume_logging_queue: {e}")

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

    for key in LOGGING_ROUTING_KEYS:
        await queue.bind(EXCHANGE_NAME, routing_key=key)
    print(f"Logging queue bound to routing keys.")
    # print(f"Logging Queue bound to exchange {EXCHANGE_NAME} with routing key {LOGGING_ROUTING_KEY}")
    return queue


async def declare_and_bind_commands(channel: aio_pika.Channel):
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

    for key in COMMAND_ROUTING_KEYS:
        await queue.bind(EXCHANGE_NAME, routing_key=key)
    print(f"Command Logging queue bound to routing keys.")
    # print(f"Command Logging Queue bound to exchange {EXCHANGE_NAME} with routing key {COMMAND_ROUTING_KEY}")
    return queue


async def save_to_redis(devId, data):
    if not redis_client:
        print("Not connected to Redis.")
        return

    try:
        max_items = 5

        key = f"logs:{devId}"

        json_data = json.dumps(data)

        await redis_client.rpush(key, json_data)

        await redis_client.ltrim(key, -max_items, -1)

    except Exception as e:
        print(f"Error in save_to_redis: {e}")


async def consume_store_redis(channel: aio_pika.Channel):
    queue = await declare_and_bind_commands(channel)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    headers = message.headers
                    body = message.body.decode()
                    data = json.loads(body)

                    if data:                      
                        full_msg = {**headers, **data} if headers else {**data}
                        
                        timestamp = data.get("timestamp", datetime.now().isoformat())
                        
                        full_msg = {"timestamp": timestamp, **full_msg}

                        device_id = get_device_id(full_msg)

                        if device_id:
                            if "heartbeat_echo" not in full_msg:
                                await save_to_redis(device_id, full_msg)

                                keys = list(full_msg.keys())
                                await save_csv_file(
                                    keys, full_msg, device_id, commandMessage=True
                                )
                except Exception as e:
                    print(f"Error in consume_store_redis: {e}")


async def main():
    await init_redis()
    while True:
        try:
            connection = await init_rabbit()
            if not connection:
                break

            async with connection:
                channel = await connection.channel()

                consumer_task = asyncio.create_task(consume_logging_queue(channel))
                redis_consumer_task = asyncio.create_task(consume_store_redis(channel))
                writer_task = asyncio.create_task(save_status_messages())

                await asyncio.gather(redis_consumer_task, consumer_task, writer_task)

        except Exception as e:
            print(f"Error: {e}. Retrying.")
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            exit()

if __name__ == "__main__":
    asyncio.run(main())
