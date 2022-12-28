import logging

import uvicorn
from fastapi import FastAPI, HTTPException

from bikeshare_data_processor.ingestion_engine import IngestionEngine

app = FastAPI()

logger = logging.getLogger('uvicorn')

handled_systems = ['capital-bikeshare']


@app.get("/")
async def root():
    return {"msg": "Micro-mobility data processor up for work"}


@app.get("/ingest/systems/{system_name}")
async def call_ingest_system_data(system_name: str):
    if system_name not in handled_systems:
        raise HTTPException(
            status_code=404,
            detail=f"System {system_name}not handled by micro-mobility platform yet"
        )
    IngestionEngine.ingest_system_data(system_name)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False, log_level="debug")
