from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import uvicorn
from pydantic import BaseModel
from typing import List

app = FastAPI()

# In-memory storage for registered NF instances (to simulate a database)
registered_nf_instances = {}

@app.get("/nnrf-disc/v1/nf-instances")
async def discover_nf_instances(
    target_nf_type: str = Query(..., alias="target-nf-type"),
    requester_nf_type: str = Query(..., alias="requester-nf-type")
):
    if target_nf_type.upper() == "NWDAF":
        response = {
            "validityPeriod": 300,
            "nfInstances": [
                {
                    "nfInstanceId": "6548c676-b19e-4018-ad41-d22cbee0b48a",
                    "nfType": "NWDAF",
                    "nfStatus": "REGISTERED",
                    "ipv4Addresses": ["NWDAF-collector"],
                }
            ]
        }
        return JSONResponse(content=response)
    else:
        return JSONResponse(
            content={"error": f"NF type '{target_nf_type}' not supported"},
            status_code=404
        )


# Define body model
class NFInstanceData(BaseModel):
    nfType: str
    nfStatus: str
    ipv4Addresses: List[str]


@app.put("/nnrf-nfm/v1/nf-instances/{nfInstanceId}")
async def register_nf_instance(nfInstanceId: str, body: NFInstanceData):

    registered_nf_instances[nfInstanceId] = {
        "nfInstanceId": nfInstanceId,
        "nfType": body.nfType,
        "nfStatus": body.nfStatus,
        "ipv4Addresses": body.ipv4Addresses
    }

    return JSONResponse(
        content=registered_nf_instances[nfInstanceId],
        status_code=201
    )


if __name__ == "__main__":
    uvicorn.run("nrf:app", host="0.0.0.0", port=8070, reload=False)
