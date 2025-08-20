# main.py
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import config

from auth import routes as auth_routes
from platforms import plat_route as platform_routes
from kafka_producer import start_producer, stop_producer

app = FastAPI(title="MultiSourceIndexer API")

# Middleware
app.add_middleware(SessionMiddleware, secret_key=config.SECRET_KEY)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.FRONTEND_URL, "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth_routes.router)
app.include_router(platform_routes.router)

# Kafka startup/shutdown hooks
@app.on_event("startup")
async def startup_event():
    await start_producer()

@app.on_event("shutdown")
async def shutdown_event():
    await stop_producer()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
