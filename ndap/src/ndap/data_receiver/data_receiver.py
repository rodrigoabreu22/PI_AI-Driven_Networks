from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json  
import uvicorn
import logging


logger = logging.getLogger(__name__)


# Kafka topic
NETWORK_TOPIC = "network_raw_data"

# FastAPI app instance
app = FastAPI()

# Kafka Producer setup
#producer = KafkaProducer(
#    bootstrap_servers="localhost:9092",
#    value_serializer=lambda v: json.dumps(v).encode("utf-8")
#)
#
# Define a data model for the network data
class NetworkItem(BaseModel):
    srcip: str
    sport: int
    dstip: str
    dsport: int
    proto: str
    state: str
    dur: float
    sbytes: int
    dbytes: int
    sttl: int
    dttl: int
    sloss: int
    dloss: int
    service: str
    Sload: float
    Dload: float
    Spkts: int
    Dpkts: int
    swin: int
    dwin: int
    stcpb: int
    dtcpb: int
    smeansz: int
    dmeansz: int
    trans_depth: int
    res_bdy_len: int
    Sjit: float
    Djit: float
    Stime: float
    Ltime: float
    Sintpkt: float
    Dintpkt: float
    tcprtt: float
    synack: float
    ackdat: float
    is_sm_ips_ports: int
    ct_state_ttl: int
    ct_flw_http_mthd: int
    is_ftp_login: int
    ct_ftp_cmd: int
    ct_srv_src: int
    ct_srv_dst: int
    ct_dst_ltm: int
    ct_src_ltm: int
    ct_src_dport_ltm: int
    ct_dst_sport_ltm: int
    ct_dst_src_ltm: int
    attack_cat: str
    Label: int

@app.post("/receive_data")
def receive_data(item: NetworkItem):
    """Receives data from the producer and sends it to Kafka."""
    try:
        #producer.send(NETWORK_TOPIC, item.model_dump())
        #producer.flush()
        logger.info("WORKING")
        return {"message": "Data successfully sent to Kafka", "data": item.dict()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending to Kafka: {str(e)}")
    

def main():
    logging.basicConfig(filename='data_receiver.log', level=logging.INFO)
    uvicorn.run(app, host="localhost", port=5002)


if __name__ == "__main__":
    main()
