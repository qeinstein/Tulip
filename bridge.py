import os
import logging
import json
import asyncio
import base64
from typing import Dict
from fastapi import FastAPI, Request, Form, Response, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from twilio.twiml.voice_response import VoiceResponse
from dotenv import load_dotenv
from openai import AsyncOpenAI
from urllib.parse import urlparse
from spitch import Spitch

load_dotenv()

app = FastAPI(title="Tulip's Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Environment variables
BASE_URL = os.getenv("BASE_URL").rstrip("/")
SPITCH_API_KEY = os.getenv("SPITCH_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
MODEL = os.getenv("model_name", "openai/gpt-4o-mini")

# Clients
openrouter_client = AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)
spitch = Spitch(api_key=SPITCH_API_KEY)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = "You are Tulip, a warm, helpful healthcare assistant. Reply concisely in the exact language the user speaks."

SPITCH_VOICE_MAP = {
    "yo": "sade",
    "ig": "ngozi",
    "ha": "aminu",
    "en": "jude"
}

LANGUAGE_MAP = {
    "1": ("Yoruba", "yo-NG", "yo"),
    "2": ("Igbo", "ig-NG", "ig"),
    "3": ("Hausa", "ha-NG", "ha"),
    "4": ("English", "en-US", "en")
}

LANGUAGE_SELECTION: Dict[str, tuple] = {}
CONVERSATION_HISTORY: Dict[str, list] = {}


def spitch_tts(text: str, voice_id: str, lang: str = "en"):
    text = text.strip()
    if not text:
        return
    response = spitch.speech.generate(
        text=text,
        language=lang,
        voice=voice_id,
        format="mp3"
    )
    audio_bytes = response.read()
    chunk_size = 4096
    for i in range(0, len(audio_bytes), chunk_size):
        yield audio_bytes[i:i + chunk_size]


@app.post("/voice")
async def voice_entry(request: Request):
    twiml = VoiceResponse()
    gather = twiml.gather(
        num_digits=1,
        action="/process_language",
        method="POST",
        timeout=10
    )
    gather.say("Welcome to Tulip. For Yoruba press 1. For Igbo press 2. For Hausa press 3. For English press 4.")
    twiml.redirect("/voice")
    return Response(content=str(twiml), media_type="application/xml")


@app.post("/process_language")
async def process_language(request: Request, Digits: str = Form(None), CallSid: str = Form(None)):
    twiml = VoiceResponse()

    if not (Digits and CallSid and Digits in LANGUAGE_MAP):
        twiml.say("Invalid selection. Returning to menu.")
        twiml.redirect("/voice")
        return Response(content=str(twiml), media_type="application/xml")

    lang_name, _, lang_code_spitch = LANGUAGE_MAP[Digits]
    LANGUAGE_SELECTION[CallSid] = (lang_name, lang_code_spitch)
    logger.info(f"Language selected: {lang_name} for CallSid {CallSid}")

    twiml.say(f"You selected {lang_name}. Connecting you to Tulip now.")

    connect = twiml.connect()
    stream = connect.stream(url=f"wss://{urlparse(BASE_URL).netloc}/mediastream")
    stream.parameter(name="tracks", value="inbound")
    stream.parameter(name="transcription", value="true")  # This gives us real-time text!

    return Response(content=str(twiml), media_type="application/xml")


@app.websocket("/mediastream")
async def mediastream(websocket: WebSocket):
    await websocket.accept()
    call_sid = None
    stream_sid = None
    interrupted = False
    current_task = None

    async def send_tts(text: str):
        nonlocal interrupted
        if not text.strip() or interrupted:
            return

        lang_name, lang_spitch = LANGUAGE_SELECTION.get(call_sid, ("English", "en"))
        voice = SPITCH_VOICE_MAP[lang_spitch]

        for chunk in spitch_tts(text, voice, lang_spitch):
            if interrupted:
                break
            payload = base64.b64encode(chunk).decode()
            await websocket.send_text(json.dumps({
                "event": "media",
                "streamSid": stream_sid,
                "media": {
                    "payload": payload
                }
            }))

        # Send mark to let Twilio know we're done
        if not interrupted:
            await websocket.send_text(json.dumps({
                "event": "mark",
                "streamSid": stream_sid,
                "mark": {"name": "end_of_tulip"}
            }))

    try:
        async for message_str in websocket.iter_text():
            message = json.loads(message_str)
            event = message.get("event")

            if event == "start":
                call_sid = message["start"]["callSid"]
                stream_sid = message["start"]["streamSid"]
                CONVERSATION_HISTORY[call_sid] = [{"role": "system", "content": SYSTEM_PROMPT}]
                logger.info(f"Call started: {call_sid}")

                # Tulip speaks FIRST — no awkward silence!
                greeting = "Hello! This is Tulip, your healthcare assistant. How can I help you today?"
                CONVERSATION_HISTORY[call_sid].append({"role": "assistant", "content": greeting})
                asyncio.create_task(send_tts(greeting))

            elif event == "media" and message["media"].get("track") == "inbound":
                # We don't use raw audio — we use transcription below
                pass

            elif event == "transcription":
                user_text = message["transcription"]["text"].strip()
                if not user_text or user_text.lower() in ["silence", "background"]:
                    continue

                logger.info(f"User ({call_sid}): {user_text}")
                interrupted = True  # Stop any ongoing speech
                await asyncio.sleep(0.2)
                interrupted = False

                history = CONVERSATION_HISTORY[call_sid]
                history.append({"role": "user", "content": user_text})

                async def respond():
                    try:
                        stream = await openrouter_client.chat.completions.create(
                            model=MODEL,
                            messages=history,
                            stream=True
                        )
                        full_reply = ""
                        async for chunk in stream:
                            delta = chunk.choices[0].delta.content or ""
                            full_reply += delta

                        if full_reply.strip():
                            history.append({"role": "assistant", "content": full_reply})
                            CONVERSATION_HISTORY[call_sid] = history[-20:]
                            await send_tts(full_reply)

                    except Exception as e:
                        logger.error(f"LLM error: {e}")
                        await send_tts("Sorry, I encountered an error. Please try again.")

                current_task = asyncio.create_task(respond())

            elif event == "stop":
                logger.info(f"Call ended: {call_sid}")
                break

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        if call_sid:
            LANGUAGE_SELECTION.pop(call_sid, None)
            CONVERSATION_HISTORY.pop(call_sid, None)