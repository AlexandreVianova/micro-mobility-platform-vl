import logging
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from bikeshare_data_processor.ingestion_engine import IngestionEngine
from bikeshare_data_processor.database_handlers import PostgresHandler as DbHandler

logger = logging.getLogger()
app = FastAPI()


@app.get("/")
async def root():
    return {"msg": "Micro-mobility data processor up for work"}


@app.get("/ingest")
async def call_ingest_system_data(system_name: str):
    if system_name not in IngestionEngine.handled_systems:
        raise HTTPException(
            status_code=400,
            detail=f"System {system_name} not handled by micro-mobility platform yet. "
                   f"Please select one of: {str(IngestionEngine.handled_systems)}"
        )
    IngestionEngine.ingest_system_data(system_name)


@app.get("/analyze/last_24h_aggs")
async def export_hourly_aggs(mode: str) -> None:
    modes = ['REST_API', 'CSV', 'JSON']
    if mode not in modes:
        raise HTTPException(
            status_code=400,
            detail=f"'mode' parameter must be one of {str(modes)}"
        )
    logger.info(f'Export required with params: mode={mode}')
    df = DbHandler.extract_hourly_aggregations()
    if mode == 'REST_API':
        return df.to_dict(orient='records')
    elif mode in ['CSV', 'JSON']:
        server_path = '/tmp/24_hours_hourly_stats'
        min_date = df['hour'].min().strftime('%Y%m%d%H')
        max_date = df['hour'].max().strftime('%Y%m%d%H')
        filename = f'hourly_stats_from_{min_date}_to_{max_date}'
        if mode == 'CSV':
            df.to_csv(server_path, index=False, sep=',')
            return FileResponse(path=server_path, filename=filename, media_type='text/csv')
        elif mode == 'JSON':
            df.to_json(server_path, orient='records')
            return FileResponse(path=server_path, filename=filename, media_type='application/json')

        msg = f"Exported hourly aggregations as {mode} file to {filename}"
        logger.info(msg)
        return {"msg": msg}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000,
                reload=False, log_level="debug")
