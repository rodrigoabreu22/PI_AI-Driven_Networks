import logging
import os
import threading
from fastapi import FastAPI, Response
from pydantic import BaseModel
from uuid import uuid4
from contextlib import asynccontextmanager
import uvicorn
from fastapi.responses import JSONResponse
import glob
import asyncio
import requests
import base64
from scapy.utils import PcapReader
from scapy.all import raw
import time

PCAP_FILE_PATH = "dataset_files"
BATCH_SIZE = 1500

packet_queue = asyncio.Queue()
subscriptions = {}
subscription_ready_event = asyncio.Event()
background_tasks = []

logging.basicConfig(
    filename='logs/nwdaf_collector.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_sorted_pcap_files():
    """Returns a sorted list of PCAP files in the dataset directory."""
    return sorted(
        glob.glob(os.path.join(PCAP_FILE_PATH, "*.pcap")),
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0])
    )

def read_all_packets(queue: asyncio.Queue):
    pcap_files = get_sorted_pcap_files()
    if not pcap_files:
        logging.info("No PCAP files found.")
        return

    for file in pcap_files:
        try:
            with PcapReader(file) as reader:
                for pkt in reader:
                    try:
                        timestamp = getattr(pkt, 'time', time.time())
                        encoded_pkt = base64.b64encode(raw(pkt)).decode("utf-8")
                        
                        # This does not require asyncio.run_coroutine_threadsafe
                        queue.put_nowait({
                            "timestamp": str(timestamp),
                            "pcap_bytes": encoded_pkt
                        })

                    except Exception as e:
                        logging.warning(f"Failed to process packet: {e}")
                        
        except Exception as e:
            logging.error(f"Failed to read {file}: {e}")

# Async workers
async def packet_producer_worker():
    logging.info("Packet producer started.")
    await asyncio.to_thread(read_all_packets, packet_queue)

async def packet_consumer_worker():
    await subscription_ready_event.wait()
    time.sleep(10)  # Allow some time for the producer to start

    logging.info("Packet consumer started.")
    batch = []

    while True:
        packet = await packet_queue.get()
        batch.append(packet)

        if len(batch) >= BATCH_SIZE:
            for sub_id, sub in subscriptions.items():
                try:
                    payload = {
                        "event": sub["event"],
                        "pcap": batch
                    }
                    response = requests.post(sub["notificationURI"], json=payload)

                    if response.status_code != 200:
                        logging.error(f"Failed to send to {sub['notificationURI']}: {response.status_code} - {response.text}")

                    time.sleep(5)  

                except Exception as e:
                    logging.error(f"Error sending batch to {sub['notificationURI']}: {e}")

            batch.clear()
        packet_queue.task_done()

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not os.path.exists(PCAP_FILE_PATH):
        raise RuntimeError(f"PCAP directory not found: {PCAP_FILE_PATH}")

    producer_task = asyncio.create_task(packet_producer_worker())
    consumer_task = asyncio.create_task(packet_consumer_worker())

    background_tasks.extend([producer_task, consumer_task])
    
    yield

    logging.info("Shutting down app.")

    for task in background_tasks:
        task.cancel()
    await asyncio.gather(*background_tasks, return_exceptions=True)

    logging.info("Background tasks cancelled.")


app = FastAPI(lifespan=lifespan)


class EventSubscription(BaseModel):
    event: str
    notificationURI: str

@app.post("/nnwdaf-eventssubscription/v1/subscriptions")
def subscribe(event_sub: EventSubscription):
    sub_id = str(uuid4())

    subscriptions[sub_id] = {
        "event": event_sub.event,
        "notificationURI": event_sub.notificationURI,
        "file_index": 0,
        "last_index": 0
    }

    location_url = f"/nnwdaf-eventssubscription/v1/subscriptions/{sub_id}"

    logging.info(f"Subscription created: {sub_id} → {event_sub.notificationURI}")
    subscription_ready_event.set()

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
        headers={"Location": location_url}
    )

@app.get("/health")
def health_check():
    return JSONResponse(
        content={"status": "OK"},
        status_code=200
    )

if __name__ == "__main__":
    uvicorn.run("nwdaf_collector:app", host="0.0.0.0", port=8071, reload=False)