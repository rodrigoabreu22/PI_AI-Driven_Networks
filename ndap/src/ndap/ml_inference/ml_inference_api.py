from fastapi import FastAPI, UploadFile, File
from fastapi import Request
from fastapi import Body
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pickle
import threading
import logging
from contextlib import asynccontextmanager
import uvicorn
from ml_inference import update_binary_model, start_kafka_inference_loop, update_multiclass_model, update_attack_mapping

# Configure logging to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/inference.log',
    filemode='a'  # 'a' to append, 'w' to overwrite each run
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting Kafka inference thread...")
    thread = threading.Thread(target=start_kafka_inference_loop, daemon=True)
    thread.start()
    yield  # Run FastAPI app
    logging.info("App shutdown. Kafka thread will stop when process exits.")

app = FastAPI(lifespan=lifespan)

@app.post("/update-model")
async def update_model(modeltr_pickle: UploadFile = File(...)):
    try:
        contents = await modeltr_pickle.read()
        model = pickle.loads(contents)
        update_binary_model(model)
        return {"message": "Model updated successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    
@app.post("/update-model-attack")
async def update_model_attack(modeltr_pickle: UploadFile = File(...)):
    try:
        contents = await modeltr_pickle.read()
        model = pickle.loads(contents)
        update_multiclass_model(model)
        return {"message": "Model updated successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.post("/update-attack-mapping")
async def update_attack_mapping_endpoint(mapping: dict = Body(...)):
    try:
        update_attack_mapping(mapping)
        return {"message": "Attack mapping updated successfully", "received": mapping}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    
if __name__ == "__main__":
    uvicorn.run("ml_inference_api:app", host="0.0.0.0", port=9050, reload=False)