from typing import List
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv

from AtlasClient import AtlasClient

load_dotenv()

client = AtlasClient(os.getenv('MONGO_URL'), os.getenv('DB_NAME'))
collection = client.get_collection('sample_restaurants')

# Initialize FastAPI app
app = FastAPI()

# TODO This needs to change based on what data the API can accept
class SearchRequest(BaseModel):
    query_vector: List[float] = Field(..., example=[0.1, -0.2, 0.3])
    top_k: int = Field(default=5, example=5)


@app.get("/")
async def ping():
    return client.ping()

@app.post("/search")
async def search(search_request: SearchRequest):
    # MongoDB Atlas vector search query
    search_query = {
        "$vectorSearch": {
            "index": "rest_nbhd_index",
            "path": "neighborhoods",
            "queryVector": search_request.query_vector,
            "numCandidates": 50,
            "limit": search_request.top_k,
        }
    }

    try:
        results = collection.aggregate([{"$search": search_query}, {"$limit": search_request.top_k}])
        search_results = [result for result in results]
        return {"results": search_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
