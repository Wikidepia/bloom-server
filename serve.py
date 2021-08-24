import uvicorn
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import PlainTextResponse
from redisbloom.client import Client

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)

rb = Client()


@app.post("/deduplicate", response_class=PlainTextResponse)
async def deduplicate(key: str, file: UploadFile = File(...)):
    if key not in ["clipped", "main", "urls"]:
        return "Invalid key"
    items = (await file.read()).decode("utf-8", "ignore").splitlines()
    ret = rb.bfMExists(key, *items)
    return "\n".join([items[idx] for idx, x in enumerate(ret) if x == 0])


@app.post("/add", response_class=PlainTextResponse)
async def add(key: str, file: UploadFile = File(...)):
    if key not in ["clipped", "main", "urls"]:
        return "Invalid key"
    items = (await file.read()).decode("utf-8", "ignore").splitlines()
    ret = rb.bfMAdd(key, *items)
    return "\n".join([items[idx] for idx, x in enumerate(ret) if x == 0])


@app.get("/info")
async def info(key: str):
    if key not in ["clipped", "main", "urls"]:
        return "Invalid key"
    info = rb.bfInfo(key)
    return {
        "inserted_num": info.insertedNum,
        "capacity": info.capacity,
        "filter_num": info.filterNum,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
