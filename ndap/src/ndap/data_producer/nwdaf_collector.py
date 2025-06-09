import logging
import os
import threading
from fastapi import FastAPI, Response
from pydantic import BaseModel
from uuid import uuid4
from callback_worker import subscription_callback_worker
from contextlib import asynccontextmanager
import uvicorn
from fastapi.responses import JSONResponse

PCAP_FILE_PATH = "dataset_files"  

logging.basicConfig(
    filename='logs/nwdaf_collector.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not os.path.exists(PCAP_FILE_PATH):
        raise RuntimeError(f"PCAP directory not found: {PCAP_FILE_PATH}")

    thread = threading.Thread(
        target=subscription_callback_worker,
        args=(subscriptions, None, subscription_ready_event),
        daemon=True
    )
    thread.start()
    logging.info("Callback worker thread started.")

    yield

    logging.info("Shutting down app.")


app = FastAPI(lifespan=lifespan)

subscriptions = {}
subscription_ready_event = threading.Event()

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