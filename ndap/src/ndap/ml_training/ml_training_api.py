from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any
import logging
import requests
from ml_training_worker import train_model_by_id  
from uuid import uuid4
import uvicorn
from fastapi.encoders import jsonable_encoder

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ml_training_api.log'),
        logging.StreamHandler()
    ]
)

app = FastAPI()

class MLModelInfoEntry(BaseModel):
    id: str

class MLModelInfo(BaseModel):
    event: str  
    addModelInfo: List[MLModelInfoEntry]

class MLEvent(BaseModel):
    event: str

class MLEventSubsc(BaseModel):
    mlEvent: MLEvent
    mlEventFilter: Dict[str, Any] = Field(default_factory=dict)

class SubscriptionRequest(BaseModel):
    mlEventSubscs: List[MLEventSubsc]
    notifUri: str
    mlModelInfos: List[MLModelInfo]
    notifCorreId: str
    mlPreFlag: bool
    mlAccChkFlg: bool

class CallbackPayload(BaseModel):
    mLModelInfos: Dict[str, Any]
    notifCorreId: str
    statusReport: Dict[str, float]


def train_and_notify(model_id: int, notif_uri: str, notif_corre_id: str, event_type: str):
    metrics = train_model_by_id(model_id)

    if "error" in metrics:
        logging.error(f"Training failed: {metrics['error']}")
        return

    callback_payload = {
        "mLModelInfos": {
            "event": event_type
        },
        "notifCorreId": notif_corre_id,
        "statusReport": metrics
    }

    try:
        response = requests.post(notif_uri, json=callback_payload)
        response.raise_for_status()
        logging.info(f"Callback sent to {notif_uri} with status: {response.status_code}")

    except requests.RequestException as e:
        logging.error(f"Failed to send callback to {notif_uri}: {e}")


@app.post("/nnwdaf-mlmodeltraining/v1/subscriptions")
async def subscribe(request: SubscriptionRequest, background_tasks: BackgroundTasks):
    try:
        model_info = request.mlModelInfos[0]
        model_id_str = model_info.addModelInfo[0].id
        model_id = int(model_id_str)  

        event_type = model_info.event

        subscription_id = str(uuid4()) 

        background_tasks.add_task(
            train_and_notify,
            model_id=model_id,
            notif_uri=request.notifUri,
            notif_corre_id=request.notifCorreId,
            event_type=event_type
        )

        response_body = jsonable_encoder(request)

        headers = {
            "Location": f"/nnwdaf-mlmodeltraining/v1/subscriptions/{subscription_id}"
        }

        return JSONResponse(content=response_body, status_code=201, headers=headers)

    except Exception as e:
        logging.error("Failed to handle subscription", exc_info=True)
        return {"error": str(e)}
    
if __name__ == "__main__":
    uvicorn.run("ml_training_api:app", host="0.0.0.0", port=8075, reload=False)
