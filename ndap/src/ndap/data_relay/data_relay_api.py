import asyncio
import logging
import uuid
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from kafka import KafkaConsumer
from contextlib import asynccontextmanager
import uvicorn
import requests
import json

BROKER = "kafka:9092"
TOPIC_RECEIVE = "INFERENCE_DATA" 
BATCH_SIZE = 50

message_queue = asyncio.Queue()
subscription_ready_event = asyncio.Event()
subscriptions = {}

logging.basicConfig(
    filename='logs/data_relay_api.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class EventSubscription(BaseModel):
    event: str
    notificationURI: str

def consume_kafka(loop):
    consumer = KafkaConsumer(
        TOPIC_RECEIVE,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    for message in consumer:
        asyncio.run_coroutine_threadsafe(
            message_queue.put(message.value),
            loop  # pass the loop captured in main thread
        )

async def kafka_consumer_worker():
    logging.info("Kafka consumer waiting for subscription...")
    await subscription_ready_event.wait()
    logging.info("Starting Kafka consumer thread...")

    loop = asyncio.get_running_loop()
    await asyncio.to_thread(consume_kafka, loop)

async def feature_dispatch_worker():
    logging.info("Feature dispatch worker running...")
    batch = []

    while True: 
        item = await message_queue.get()
        batch.append(item)

        if len(batch) >= BATCH_SIZE:
            for sub_id, sub in subscriptions.items():
                try:
                    payload = {
                        "event": sub["event"],
                        "features": batch
                    }

                    response = requests.post(sub["notificationURI"], json=payload)

                    if response.status_code != 200:
                        logging.error(f"Failed to send batch to {sub['notificationURI']}: {response.status_code} - {response.text}")

                except Exception as e:
                    logging.error(f"Error sending batch to {sub['notificationURI']}: {e}")
            batch.clear()

        message_queue.task_done()

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(kafka_consumer_worker())
    asyncio.create_task(feature_dispatch_worker())

    yield

    logging.info("Shutting down app.")

app = FastAPI(lifespan=lifespan)

@app.post("/nnwdaf-eventssubscription/v1/subscriptions")
async def subscribe(event_sub: EventSubscription):
    sub_id = str(uuid.uuid4())

    subscriptions[sub_id] = {
        "event": event_sub.event,
        "notificationURI": event_sub.notificationURI
    }

    subscription_ready_event.set()

    logging.info(f"Subscription created: {sub_id} → {event_sub.notificationURI}")

    return JSONResponse(
        content={
            "eventSubscriptions": [
                {
                    "event": event_sub.event,
                    "notificationURI": event_sub.notificationURI
                }
            ]
        },
        status_code=201,
        headers={"Location": f"/nnwdaf-eventssubscription/v1/subscriptions/{sub_id}"}
    )

@app.get("/health")
async def health_check():
    return {"status": "OK"}

if __name__ == "__main__":
    uvicorn.run("data_relay_api:app", host="0.0.0.0", port=8073, reload=False)