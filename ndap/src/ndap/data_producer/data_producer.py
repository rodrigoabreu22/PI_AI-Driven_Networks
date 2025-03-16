from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import requests
import time
import logging

# FastAPI app instance
app = FastAPI()

# URL of the data receiver service
DATA_RECEIVER_URL = "http://localhost:5000/receive_data"  # Change this to match the actual receiver's API

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
    Stime: float #time?
    Ltime: float #time?
    Sintpkt: float
    Dintpkt: float
    tcprtt: float
    synack: float
    ackdat: float
    is_sm_ips_ports: int #binary?
    ct_state_ttl: int
    ct_flw_http_mthd: int
    is_ftp_login: int #binary?
    ct_ftp_cmd: int
    ct_srv_src: int
    ct_srv_dst: int 
    ct_dst_ltm: int
    ct_src_ltm: int
    ct_src_dport_ltm: int
    ct_dst_sport_ltm: int
    ct_dst_src_ltm: int
    attack_cat: str
    Label: int #binary?


def get_data():
    """Reads network data from CSV and returns a list of NetworkItem objects."""
    try:
        logging.info("Starting to read data from CSV...")
        
        # Read CSV without headers (header=None)
        df = pd.read_csv("UNSW-NB15_1.csv", header=None, low_memory=False)

        df = df.dropna(how='all')

        # Debug: Check the first 3 rows to see the structure
        logging.info(f"First 3 rows from CSV: {df.head(3)}")

        # Create a list of required column indices based on the NetworkItem model
        # Adjust the order according to your CSV structure
        data_items = []
        for index, row in df.iterrows():
            # Replace NaN values with 'unknown' or any other default string
            item = {
                "srcip": row[0] if pd.notna(row[0]) else "unknown",
                "sport": row[1] if pd.notna(row[1]) else 0,
                "dstip": row[2] if pd.notna(row[2]) else "unknown",
                "dsport": row[3] if pd.notna(row[3]) else 0,
                "proto": row[4] if pd.notna(row[4]) else "unknown",
                "state": row[5] if pd.notna(row[5]) else "unknown",
                "dur": row[6] if pd.notna(row[6]) else 0.0,
                "sbytes": row[7] if pd.notna(row[7]) else 0,
                "dbytes": row[8] if pd.notna(row[8]) else 0,
                "sttl": row[9] if pd.notna(row[9]) else 0,
                "dttl": row[10] if pd.notna(row[10]) else 0,
                "sloss": row[11] if pd.notna(row[11]) else 0,
                "dloss": row[12] if pd.notna(row[12]) else 0,
                "service": row[13] if pd.notna(row[13]) else "unknown",
                "Sload": row[14] if pd.notna(row[14]) else 0.0,
                "Dload": row[15] if pd.notna(row[15]) else 0.0,
                "Spkts": row[16] if pd.notna(row[16]) else 0,
                "Dpkts": row[17] if pd.notna(row[17]) else 0,
                "swin": row[18] if pd.notna(row[18]) else 0,
                "dwin": row[19] if pd.notna(row[19]) else 0,
                "stcpb": row[20] if pd.notna(row[20]) else 0,
                "dtcpb": row[21] if pd.notna(row[21]) else 0,
                "smeansz": row[22] if pd.notna(row[22]) else 0,
                "dmeansz": row[23] if pd.notna(row[23]) else 0,
                "trans_depth": row[24] if pd.notna(row[24]) else 0,
                "res_bdy_len": row[25] if pd.notna(row[25]) else 0,
                "Sjit": row[26] if pd.notna(row[26]) else 0.0,
                "Djit": row[27] if pd.notna(row[27]) else 0.0,
                "Stime": row[28] if pd.notna(row[28]) else 0.0,
                "Ltime": row[29] if pd.notna(row[29]) else 0.0,
                "Sintpkt": row[30] if pd.notna(row[30]) else 0.0,
                "Dintpkt": row[31] if pd.notna(row[31]) else 0.0,
                "tcprtt": row[32] if pd.notna(row[32]) else 0.0,
                "synack": row[33] if pd.notna(row[33]) else 0.0,
                "ackdat": row[34] if pd.notna(row[34]) else 0.0,
                "is_sm_ips_ports": row[35] if pd.notna(row[35]) else 0,
                "ct_state_ttl": row[36] if pd.notna(row[36]) else 0,
                "ct_flw_http_mthd": row[37] if pd.notna(row[37]) else 0,
                "is_ftp_login": row[38] if pd.notna(row[38]) else 0,
                "ct_ftp_cmd": row[39] if pd.notna(row[39]) else 0,
                "ct_srv_src": row[40] if pd.notna(row[40]) else 0,
                "ct_srv_dst": row[41] if pd.notna(row[41]) else 0,
                "ct_dst_ltm": row[42] if pd.notna(row[42]) else 0,
                "ct_src_ltm": row[43] if pd.notna(row[43]) else 0,
                "ct_src_dport_ltm": row[44] if pd.notna(row[44]) else 0,
                "ct_dst_sport_ltm": row[45] if pd.notna(row[45]) else 0,
                "ct_dst_src_ltm": row[46] if pd.notna(row[46]) else 0,
                "attack_cat": row[47] if pd.notna(row[47]) else "unknown",  # Replacing NaN with 'unknown'
                "Label": row[48] if pd.notna(row[48]) else 0
            }
            logging.info(f"{index} Successfully read")
            data_items.append(item)

        # Convert the list of dicts into NetworkItem objects
        #network_items = [
        #    NetworkItem(**item) 
        #    for item in data_items]
        
        network_items = []
        i=0
        for item in data_items:
            if item["dsport"]=="0xc0a8":
                continue
            i+=1
            logging.info({i})
            network_items.append(NetworkItem(**item))

        logging.info(f"Successfully read {len(network_items)} items from CSV.")
        return network_items
    except Exception as e:
        logging.error(f"Error reading CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")



def post_data(item: NetworkItem):
    """Sends network data to the external data receiver service in JSON format."""
    try:
        
        headers = {"Content-Type": "application/json"}  # Ensuring JSON format
        response = requests.post(DATA_RECEIVER_URL, json=item.model_dump(), headers=headers)
        response.raise_for_status()  # Raise an error for failed requests
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to send data: {str(e)}")

@app.get("/send_data")
def send_data():
    """Reads data from CSV and sends it to the external data receiver in JSON format."""
    try:
        data_items = get_data()
        batch_size = 1000  # Send data in batches of 1000
        
        for i in range(0, len(data_items), batch_size):
            batch = data_items[i:i+batch_size]
            for item in batch:
                post_data(item)  # Send each row as a JSON object
            time.sleep(1)  # Pause to avoid overloading the receiver

        return {"message": "Data sent successfully in JSON format"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

#def main():
#    logging.basicConfig(filename='data_producer.log', level=logging.INFO)
#    uvicorn.run(app, host="localhost", port=5001)
#    get_data()
def main():
    logging.basicConfig(filename='data_producer.log', level=logging.INFO)
    get_data()


if __name__ == "__main__":
    main()



