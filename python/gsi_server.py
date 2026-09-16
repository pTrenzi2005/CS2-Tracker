import uvicorn
from fastapi import FastAPI, Request
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.eventhub import EventHubProducerClient, EventData
import json

from config import KV_URI, EVENT_HUB_NAME, EVH_CONN_STRING_SECRET

#AZURE CREDENTIAL
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=KV_URI, credential=credential)
conn_string = secret_client.get_secret(EVH_CONN_STRING_SECRET).value

#EVENT HUB
producer = EventHubProducerClient.from_connection_string(
    conn_str=conn_string,
    eventhub_name=EVENT_HUB_NAME
)

#FAST API
app = FastAPI()

data_packets_sent = 0
data_packets_lost = 0

@app.post("/")
async def gsi_endpoint(request: Request):
    try:
        data = await request.json()
        
        if data:
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(body=json.dumps(data)))
            producer.send_batch(event_data_batch=event_data_batch)
            print("Data sent to Event Hub")

            data_packets+=1
            
    except Exception:
        data_packets_lost += 1
        pass

    return {"status": "success"}

if __name__ == '__main__':
    print("GSI server is running...")
    uvicorn.run(app, host="127.0.0.1", port=3000, log_level="warning")

    print(f"Total data packets sent to Event Hub: {data_packets_sent}")
    print(f"Total data packets lost: {data_packets_lost}")