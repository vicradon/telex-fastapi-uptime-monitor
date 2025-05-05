import os
import asyncio
import json
import datetime
from fastapi import FastAPI, BackgroundTasks, Request, Body
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import httpx
import random
import redis
from enum import Enum
from dotenv import load_dotenv

load_dotenv()

r = redis.Redis(
    host=os.getenv("redis_host"),
    port=int(os.getenv("redis_port")),
    decode_responses=True,
    username=os.getenv("redis_username"),
    password=os.getenv("redis_password"),
)


telex_keys_key = "telex_api_keys"


class Setting(BaseModel):
    label: str
    type: str
    required: bool
    default: str


class MonitorPayload(BaseModel):
    channel_id: str
    return_url: str
    settings: List[Setting]


class AuthCallbackPayload(BaseModel):
    api_key: Optional[str]
    org_id: Optional[str]


app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://staging.telextest.im",
        "http://telextest.im",
        "https://staging.telex.im",
        "https://telex.im",
    ],  # NB: telextest is a local url
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/logo")
def get_logo():
    return FileResponse("uptime.png")


@app.get("/integration.json")
def get_integration_json(request: Request):
    base_url = str(request.base_url).rstrip("/")

    integration_json = {
        "data": {
            "date": {"created_at": "2025-02-09", "updated_at": "2025-02-09"},
            "descriptions": {
                "app_name": "Uptime Monitor",
                "app_description": "A local uptime monitor",
                "app_logo": "https://i.imgur.com/lZqvffp.png",
                "app_url": base_url,
                "background_color": "#fff",
            },
            "auth_callback": f"{base_url}/auth_callback",
            "bot": True,
            "bot_profile": {
                "name": "Uptimer bot",
            },
            "is_active": False,
            "integration_type": "interval",
            "key_features": ["- monitors websites"],
            "integration_category": "Monitoring & Logging",
            "author": "Osinachi Chukwujama",
            "website": base_url,
            "settings": [
                {"label": "site-1", "type": "text", "required": True, "default": ""},
                {"label": "site-2", "type": "text", "required": True, "default": ""},
                {
                    "label": "interval",
                    "type": "text",
                    "required": True,
                    "default": "0 * * * *",
                },
            ],
            "target_url": f"{base_url}/receive_message",
            "tick_url": f"{base_url}/tick",
        }
    }

    return integration_json


async def check_site_status(
    site: str, max_retries: int = 3, timeout: float = 10.0
) -> Optional[str]:
    transport = httpx.AsyncHTTPTransport(retries=max_retries)

    # Configure client with retry transport
    async with httpx.AsyncClient(
        transport=transport, timeout=timeout, follow_redirects=True
    ) as client:
        try:
            response = await client.get(site)

            # Check if response indicates success
            if 200 <= response.status_code < 400:
                return None  # Site is up

            return f"Site {site} is down (HTTP {response.status_code})"

        except httpx.TimeoutException:
            return f"Site {site} timed out after {timeout} seconds"

        except httpx.HTTPError as e:
            return f"Site {site} is down (HTTP Error: {str(e)})"

        except httpx.TransportError as e:
            return f"Site {site} is down (Transport Error: {str(e)})"

        except Exception as e:
            return f"Site {site} is down (Unexpected Error: {str(e)})"


async def monitor_task(payload: MonitorPayload):
    """Background task to monitor sites and send results."""
    sites = []

    for setting in payload.settings:
        if setting.label.startswith("site"):
            sites.append(setting.default)

    results = await asyncio.gather(*(check_site_status(site) for site in sites))

    results = "\n".join([res for res in results if isinstance(res, str)])

    telex_format = {
        "message": results,
        "username": "Uptime Monitor",
        "event_name": "Uptime Check",
        "status": "error",
    }

    headers = {"Content-Type": "application/json"}

    if results:
        async with httpx.AsyncClient() as client:
            res = await client.post(
                payload.return_url, json=telex_format, headers=headers
            )


@app.post("/tick", status_code=202)
def monitor(payload: MonitorPayload, background_tasks: BackgroundTasks):
    """Immediately returns 202 and runs monitoring in the background."""
    background_tasks.add_task(monitor_task, payload)
    return {"status": "success"}


class MessageType(Enum):
    THREAD = "message"
    REGULAR = "message/thread"


class NewMessagePayload(BaseModel):
    message: str
    message_type: Optional[MessageType] = None
    settings: Optional[List[Dict[str, Any]]] = None
    org_id: Optional[str] = None
    channel_id: Optional[str] = None
    thread_id: Optional[str] = None
    media: Optional[List[Dict[str, Any]]] = None
    mentions: Optional[List[Dict[str, Any]]] = None
    is_mentioned: Optional[bool] = None
    is_dm: Optional[bool] = False

    class Config:
        extra = "allow"


def send_back_to_telex(payload: NewMessagePayload):
    sendback_uri = f"https://ping.staging.telex.im/v1/return/{payload.channel_id}"
    api_key = ""
    headers = {}
    if payload.org_id:
        api_key = r.hget(telex_keys_key, payload.org_id)

    goofy_responses = [
        f"Hehe, I'm da uptimer. The datetime is {datetime.datetime.now().isoformat()} and the Sun's probably shining somewhere else in the world.",
        f"Yo! I just pinged a random IP and it winked back. Probably means it's up. Time now: {datetime.datetime.utcnow().isoformat()}Z.",
        "Uhh, I saw a 200 OK fly by in the logs... so yeah, some server somewhere still breathes!",
        f"STATUS UPDATE: The time is {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} and I definitely just misread a timeout as success.",
        "Bleep bloop, uptime is technically non-zero. Parsing request failed... but aren't we all failing, really?",
        f"I scanned some ports, I saw some lights blink. That's all I got. Timestamp: {datetime.datetime.now().ctime()}",
        "Good news: I didn't crash. Bad news: I have no idea what you asked me. Here’s some fake uptime: 99.98%",
        f"Just ran 'uptime' on myself. Turns out I'm sentient spaghetti code. Current time: {datetime.datetime.now().isoformat()}",
        "I pinged a fridge and it hummed back. Uptime confirmed. Or maybe it was the dishwasher.",
        f"According to my latest diagnostic: 1 out of 5 servers agree I'm doing something. Current Unix timestamp: {int(datetime.datetime.now().timestamp())}",
    ]

    message = random.choice(goofy_responses)

    reply_json = {"message": message}

    # TODO: This was done for backwards compability, remove when enforcing API keys
    if api_key:
        headers = {"X-TELEX-API-KEY": api_key}
    else:
        reply_json["username"] = "Uptimer Bot"

    if payload.message_type == MessageType.THREAD:
        reply_json["reply"] = True
        reply_json["thread_id"] = payload.thread_id

    res = httpx.post(
        sendback_uri,
        headers=headers,
        json=reply_json,
    )

    if res.status_code < 400 and res.status_code >= 200:
        if reply_json.get("reply"):
            print("successfully sent thread message back to telx")
        else:
            print("successfully sent message back to telx")
    else:
        print("Failed to send message back to Telex", res.text)


def complete_auth_exchange(payload: AuthCallbackPayload):
    url = "https://api.staging.telex.im/api/v1/agents/callback"
    headers = {"X-TELEX-API-KEY": payload.api_key or ""}

    response = httpx.get(url, headers=headers)

    if response.status_code < 400:
        r.hset(telex_keys_key, payload.org_id, payload.api_key)
    return response


# NewMessagePayload
@app.post("/receive_message")
async def receive_message(
    payload: NewMessagePayload, background_tasks: BackgroundTasks
):
    background_tasks.add_task(send_back_to_telex, payload)
    return {"status": "success", "message": "thank you Telex for your message"}


@app.post("/auth_callback", status_code=200)
def handle_auth_callback(
    payload: AuthCallbackPayload, background_tasks: BackgroundTasks
):
    """call back telex using the API key"""
    background_tasks.add_task(complete_auth_exchange, payload)
    return {"status": "success", "payload": payload}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
