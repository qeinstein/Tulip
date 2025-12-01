import os
import logging
import json
import asyncio
import requests
import base64
from typing import Dict, Any, Iterator
from fastapi import FastAPI, Request, Form, HTTPException, WebSocket, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from twilio.twiml.voice_response import VoiceResponse
from twilio.request_validator import RequestValidator
from dotenv import load_dotenv
from openai import AsyncOpenAI
from urllib.parse import urlparse
from starlette.websockets import WebSocketDisconnect, WebSocketState

app = FastAPI(title = "Tulip's Backend")
load_dotenv()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],

)


BASE_URL = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")
print(BASE_URL)
SPITCH_API_KEY = os.getenv("SPITCH_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
MODEL = os.getenv("model_name")


required_vars = [
    "SPITCH_API_KEY",
    "OPENROUTER_API_KEY",
    "TWILIO_ACCOUNT_SID",
    "TWILIO_AUTH_TOKEN",
    "CONVERSATION_SERVICE_SID",
]
for var in required_vars:
    if not os.getenv(var):
        logging.warning(f"Missing environment variable: {var}. The application may not function correctly.")

 
openrouter_client = AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
app = FastAPI()
twilio_validator = RequestValidator(TWILIO_AUTH_TOKEN)

SYSTEM_PROMPT = (
    "You are a helpful healthcare assistant named Tulip. You are expected to reply in the exact language as the user. Keep your responses concise."
)

SPITCH_VOICE_MAP = {
    "yo": "sade",
    "ig": "ngozi",
    "ha": "aminu",
    "en": "jude"
}

# map for twi languages
LANGUAGE_MAP = {
    "1": ("Yoruba", "yo-NG", "yo"),
    "2": ("Igbo", "ig-NG", "ig"),
    "3": ("Hausa", "ha-NG", "ha"),
    "4": ("English", "en-US", "en")
}

LANGUAGE_SELECTION: Dict[str, tuple] = {}  # CallSid -> (lang_name, lang_code_twiml, lang_code_spitch)
CONVERSATION_HISTORY: Dict[str, list] = {}  # CallSid -> history



def spitch_tts(text: str, voice_id: str, lang: str = "en") -> Iterator[bytes]:

    url = "https://api.spi-tch.com/v1/synthesize"
    headers = {
        "Authorization": f"Bearer {SPITCH_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "language": lang,
        "voice": voice_id,
        "text": text
    }
    
    try:
        resp = requests.post(
            url, 
            headers=headers, 
            json=payload, 
            stream=True
        )
        
        resp.raise_for_status() 

    except Exception as e:
        logger.error(f"Spitch TTS request failed. Payload={payload}", exc_info=True)
        raise

    for chunk in resp.iter_content(chunk_size=4096): # Using a slightly larger chunk size for better audio streaming performance
        if chunk:
            yield chunk



@app.post("/voice")
async def voice_entry(request: Request):
    print("mo to debi 1")
    form_data = await request.form()
    print("before signature")
    signature = request.headers.get("X-Twilio-Signature", "")
    print("before url")

    url = str(request.url)
    print("before validator")
    # validation_url = f"{BASE_URL}/voice"
    # if not twilio_validator.validate(validation_url, dict(form_data), signature):
    #     raise HTTPException(status_code=403, detail="Invalid Twilio signature")


    print("before vr")
    twiml = VoiceResponse()
    print("mo to debi 2")

    gather = twiml.gather(
        num_digits=1,
        action="/process_language",
        method="POST",
        timeout=8
    )
    print("mo to debi 3")

    gather.say("Welcome to Tulip. For Yoruba press 1. For Igbo press 2. For Hausa press 3. For English press 4.")
    twiml.redirect("/process_language_fallback")
    return Response(content=str(twiml), media_type="application/xml")

@app.post("/process_language_fallback")
async def process_language_fallback(request: Request):
    # Validation logic omitted for brevity in fallbacks, but should be included
    twiml = VoiceResponse()
    twiml.say("Sorry, we did not receive input. Redirecting you back to language selection.")
    twiml.redirect("/voice")
    return Response(content=str(twiml), media_type="application/xml")

@app.post("/process_language")
async def process_language(request: Request, Digits: str = Form(None), CallSid: str = Form(None)):
    form_data = await request.form()
    signature = request.headers.get("X-Twilio-Signature", "")
    url = str(request.url)
    
    validation_url = f"{BASE_URL}/process_language"
    if not twilio_validator.validate(validation_url, dict(form_data), signature):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    twiml = VoiceResponse()
    if not (Digits and CallSid and Digits in LANGUAGE_MAP):
        twiml.say("Invalid selection or call ID. Please try again.")
        twiml.redirect("/voice")
        return Response(content=str(twiml), media_type="application/xml")

    lang_name, lang_code_twiml, lang_code_spitch = LANGUAGE_MAP[Digits]
    LANGUAGE_SELECTION[CallSid] = (lang_name, lang_code_twiml, lang_code_spitch)
    logger.info("Language set for CallSid %s -> %s", CallSid, lang_name)

    twiml.say(f"You selected {lang_name}. Connecting you now.")

    # The Conversation Relay endpoint must be publicly accessible (e.g., via Ngrok/BASE_URL)
    connect = twiml.connect()
    conversation_relay = connect.conversation_relay(
        url=f"wss://{urlparse(BASE_URL).netloc}/relay",
        interruptible="any",
        report_input_during_agent_speech="any",
        debug="speaker-events"
    )

    # Note on STT: This Language block only hints to Twilio's STT provider, 
    # which may not have high-quality models for non-English languages.
    if lang_code_twiml == "en-US":
        conversation_relay.language(
            code="en-US",
            transcription_provider="google"
        )
    # For the non-English languages, we rely on the LLM's translation 
    # and Spitch TTS/STT if you switch to Media Streams.
    
    return Response(content=str(twiml), media_type="application/xml")



@app.websocket("/relay")
async def relay_websocket(websocket: WebSocket):
    await websocket.accept()
    call_sid = None
    message_queue = asyncio.Queue()
    interrupted = False
    current_response_task = None

    async def receiver():
        while True:
            try:
                data = await websocket.receive_text()
                await message_queue.put(json.loads(data))
            except (WebSocketDisconnect, RuntimeError): # RuntimeError for graceful shutdown
                await message_queue.put(None)
                break
            except Exception as e:
                logger.error(f"Receiver error: {e}")
                await message_queue.put(None)
                break

    receive_task = asyncio.create_task(receiver())

    try:
        while True:
            message = await message_queue.get()
            if message is None:
                break

            event_type = message.get("type")
            
            if event_type == "setup":
                call_sid = message.get("callSid")
                CONVERSATION_HISTORY[call_sid] = [{"role": "system", "content": SYSTEM_PROMPT}]
                logger.info(f"Setup received for CallSid: {call_sid}")
                continue

            elif event_type == "prompt":
                user_text = message.get("voicePrompt")
                if not user_text or not user_text.strip():
                    continue

                if current_response_task and not current_response_task.done():
                    interrupted = True
                    await asyncio.sleep(0.1) # Give task a moment to acknowledge interruption
                
                _, _, lang_spitch = LANGUAGE_SELECTION.get(call_sid, ("English", "en-US", "en"))


                # if lang_spitch != "en":
                #     try:
                #         english_text = spitch_translate(user_text, source=lang_spitch, target="en")
                #     except Exception as e:
                #         logger.error(f"Translation error (input): {e}")
                #         english_text = user_text
                # else:
                english_text = user_text

                history = CONVERSATION_HISTORY.get(call_sid, [{"role": "system", "content": SYSTEM_PROMPT}])
                history.append({"role": "user", "content": english_text})
                interrupted = False

                async def stream_response():
                    nonlocal interrupted, history
                    reply_en = ""
                    
                    try:
                        # 1. STREAM LLM RESPONSE AND COLLECT FULL TEXT
                        stream = await openrouter_client.chat.completions.create(
                            model=MODEL,
                            messages=history,
                            stream=True
                        )
                        async for chunk in stream:
                            if interrupted:
                                logger.info("LLM stream interrupted.")
                                break
                            delta = chunk.choices[0].delta.content or ""
                            if delta:
                                reply_en += delta
                        
                        if interrupted or not reply_en:
                            return # Stop if interrupted or no reply was generated

                        if lang_spitch != "en":
                            try:
                                reply_local = spitch_translate(reply_en, source="en", target=lang_spitch)
                            except Exception as e:
                                logger.error(f"Translation error (output): {e}")
                                reply_local = reply_en
                        else:
                            reply_local = reply_en
                        
                        logger.info(f"Final Reply (Local): {reply_local}")

                        # 3. GENERATE AND STREAM AUDIO CHUNKS
                        voice_for_lang = SPITCH_VOICE_MAP.get(lang_spitch, SPITCH_VOICE_MAP["en"])
                        audio_stream_generator = spitch_tts(reply_local, voice_for_lang, lang_spitch)
                        
                        for audio_chunk in audio_stream_generator:
                            if interrupted:
                                logger.info("Audio stream interrupted.")
                                break
                            
                            # CRITICAL FIX: Base64 encode the audio chunk and send as "audio" type
                            base64_audio = base64.b64encode(audio_chunk).decode('utf-8')
                            
                            await websocket.send_text(
                                json.dumps({
                                    "type": "audio",
                                    "audio": base64_audio,
                                    "last": False # Indicate more chunks are coming
                                })
                            )

                        # 4. FINAL CLEANUP AND HISTORY UPDATE
                        if not interrupted:
                            # Send final empty audio chunk to signal the end of the TTS stream
                            await websocket.send_text(
                                json.dumps({
                                    "type": "audio",
                                    "audio": "",
                                    "last": True
                                })
                            )
                            # Update history only after successful completion
                            history.append({"role": "assistant", "content": reply_en})
                            CONVERSATION_HISTORY[call_sid] = history[-20:] # Keep last 20 messages

                    except Exception as e:
                        logger.error(f"Error in stream_response: {e}")
                        # Ensure the conversation is terminated gracefully on error
                        if not interrupted:
                            await websocket.send_text(json.dumps({"type": "audio", "audio": "", "last": True}))


                current_response_task = asyncio.create_task(stream_response())

            elif event_type == "speaker":
                if message.get("event") == "clientSpeaking":
                    # Interrupt ongoing TTS/LLM generation if the user starts speaking
                    interrupted = True
                continue

            elif event_type == "call_ended":
                logger.info(f"Call {call_sid} ended. Cleaning up.")
                LANGUAGE_SELECTION.pop(call_sid, None)
                CONVERSATION_HISTORY.pop(call_sid, None)
                break

    except Exception as e:
        logger.error(f"Outer WebSocket handler error: {e}")
    finally:
        if current_response_task:
            current_response_task.cancel()
        receive_task.cancel()
        if call_sid:
            LANGUAGE_SELECTION.pop(call_sid, None)
            CONVERSATION_HISTORY.pop(call_sid, None)
        try:
            # Check connection state before trying to close (WebSocketState import is needed)
            if websocket.client_state != WebSocketState.DISCONNECTED:
                await websocket.close()
        except Exception:
            pass

























# import os
# import logging
# import json
# import asyncio
# import requests
# import base64
# from typing import Dict, Any, Iterator
# from fastapi import FastAPI, Request, Form, HTTPException, WebSocket, WebSocketDisconnect
# from fastapi.responses import Response
# from twilio.twiml.voice_response import VoiceResponse
# from twilio.request_validator import RequestValidator
# from dotenv import load_dotenv
# from spitch import Spitch
# from openai import AsyncOpenAI
# from urllib.parse import urlparse


# load_dotenv()

# required_vars = [
#     "SPITCH_API_KEY",
#     "OPENROUTER_API_KEY",
#     "TWILIO_ACCOUNT_SID",
#     "TWILIO_AUTH_TOKEN",
#     # "BASE_URL",
#     "CONVERSATION_SERVICE_SID",
# ]
# for var in required_vars: #getting everything at once to avoid rewriting over and over
#     if not os.getenv(var):
#         raise RuntimeError(f"Missing environment variable: {var}")

# SPITCH_API_KEY = os.getenv("SPITCH_API_KEY")
# OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
# TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
# # BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
# MODEL = os.getenv("model_name")


# SYSTEM_PROMPT = (
#     "You are a helpful healtcare assistant named Tulip, youa re expected to reply in the exact language as the user with the correct annotations"
# )

# SPITCH_VOICE_MAP = {
#     "yo": "sade",   # e.g. "spitch_yo_female_1"
#     "ig": "ngozi",
#     "ha": "aminu",
#     "en": "jude"   # maybe you want a Spitch voice for English too
# }

# spitch_client = Spitch(api_key=SPITCH_API_KEY)
# openrouter_client = AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)


# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)
# # logger = logging.getLogger("conversation-relay")
# app = FastAPI()
# twilio_validator = RequestValidator(TWILIO_AUTH_TOKEN)

# #map for twi languages
# LANGUAGE_MAP = {
#     "1": ("Yoruba", "yo-NG", "yo"),
#     "2": ("Igbo", "ig-NG", "ig"),
#     "3": ("Hausa", "ha-NG", "ha"),
#     "4": ("English", "en-US", "en")
# }

# LANGUAGE_SELECTION: Dict[str, tuple] = {}  # CallSid -> (lang_name, lang_code_twiml, lang_code_spitch)
# CONVERSATION_HISTORY: Dict[str, list] = {}  # CallSid -> history


# def spitch_tts(text: str, voice_id: str, lang: str = "en") -> Iterator[bytes]:

#     url = "https://api.spi-tch.com/v1/synthesize"
#     headers = {
#         "Authorization": f"Bearer {{SPITCH_API_KEY}}",
#         "Content-Type": "application/json"
#     }
#     payload = {
#         "language": lang,
#         "voice": voice_id,
#         "text": text
#     }
    
#     try:
#         resp = requests.post(
#             url, 
#             headers=headers, 
#             json=payload, 
#             stream=True #to be able to stream audio
#         )
        
#         resp.raise_for_status() 

#     except Exception as e:

#         logging.error(f"Spitch TTS request failed. Payload={payload}", exc_info=True)
#         raise


#     for chunk in resp.iter_content(chunk_size=1024):
#         if chunk: # Filter out keep-alive chunks
#             yield chunk


# @app.post("/voice")
# async def voice_entry(request: Request):
#     form_data = await request.form()
#     signature = request.headers.get("X-Twilio-Signature", "")
#     url = str(request.url)
#     if not twilio_validator.validate(url, dict(form_data), signature):
#         raise HTTPException(status_code=403, detail="Invalid Twilio signature")

#     twiml = VoiceResponse()
#     gather = twiml.gather(
#         num_digits=1,
#         action="/process_language",
#         method="POST",
#         timeout=8
#     )
#     gather.say("Welcome to Tulip. For Yoruba press 1. For Igbo press 2. For Hausa press 3. For English press 4.")
#     twiml.redirect("/process_language_fallback")
#     return Response(content=str(twiml), media_type="application/xml")

# @app.post("/process_language_fallback")
# async def process_language_fallback(request: Request):
#     twiml = VoiceResponse()
#     twiml.say("Sorry, we did not receive input. Redirecting you back to language selection.")
#     twiml.redirect("/voice")
#     return Response(content=str(twiml), media_type="application/xml")

# @app.post("/process_language")
# async def process_language(request: Request, Digits: str = Form(None), CallSid: str = Form(None)):
#     form_data = await request.form()
#     signature = request.headers.get("X-Twilio-Signature", "")
#     url = str(request.url)
#     if not twilio_validator.validate(url, dict(form_data), signature):
#         raise HTTPException(status_code=403, detail="Invalid Twilio signature")

#     twiml = VoiceResponse()
#     if not (Digits and CallSid and Digits in LANGUAGE_MAP):
#         twiml.say("Invalid selection or call ID. Please try again.")
#         twiml.redirect("/voice")
#         return Response(content=str(twiml), media_type="application/xml")

#     lang_name, lang_code_twiml, lang_code_spitch = LANGUAGE_MAP[Digits]
#     LANGUAGE_SELECTION[CallSid] = (lang_name, lang_code_twiml, lang_code_spitch)
#     logger.info("Language set for CallSid %s -> %s", CallSid, lang_name)

#     twiml.say(f"You selected {lang_name}. Connecting you now.")

#     connect = twiml.connect()
#     conversation_relay = connect.conversation_relay(
#         url=f"wss://{urlparse(BASE_URL).netloc}/relay",
#         interruptible="any",
#         report_input_during_agent_speech="any",
#         debug="speaker-events"
#     )

#     if lang_code_twiml == "en-US":
#         conversation_relay.language(
#             code="en-US",
#             transcription_provider="google"
#             # i could also include tts_provider if English via Twilio,
#             # but since we’re using Spitch for all languages i might skip tts_provider here
#         )
#     else:
#         logger.info("Skipping <Language> block for %s", lang_name)

#     return Response(content=str(twiml), media_type="application/xml")


# @app.websocket("/relay")
# async def relay_websocket(websocket: WebSocket):
#     await websocket.accept()
#     call_sid = None
#     message_queue = asyncio.Queue()
#     interrupted = False
#     current_response_task = None

#     async def receiver():
#         while True:
#             try:
#                 data = await websocket.receive_text()
#                 await message_queue.put(json.loads(data))
#             except WebSocketDisconnect:
#                 await message_queue.put(None)
#                 break
#             except Exception as e:
#                 logger.error(f"Receiver error: {e}")
#                 await message_queue.put(None)
#                 break

#     receive_task = asyncio.create_task(receiver())

#     try:
#         while True:
#             message = await message_queue.get()
#             if message is None:
#                 break

#             event_type = message.get("type")
#             logger.info("event type is %s", event_type)

#             if event_type == "setup":
#                 call_sid = message.get("callSid")
#                 CONVERSATION_HISTORY[call_sid] = [{"role": "system", "content": SYSTEM_PROMPT}]
#                 continue

#             elif event_type == "prompt":
#                 user_text = message.get("voicePrompt")
#                 if not user_text or not user_text.strip():
#                     continue

#                 if current_response_task and not current_response_task.done():
#                     interrupted = True
#                     await asyncio.sleep(0)

#                 _, _, lang_spitch = LANGUAGE_SELECTION.get(call_sid, ("English", "en-US", "en"))

#                 # Translate input from local language to English if needed
#                 if lang_spitch != "en":
#                     try:
#                         english_text = spitch_translate(user_text, source=lang_spitch, target="en")
#                     except Exception as e:
#                         logger.error(f"Translation error (input): {e}")
#                         # fallback to user_text if translation fails
#                         english_text = user_text
#                 else:
#                     english_text = user_text

#                 history = CONVERSATION_HISTORY.get(call_sid, [{"role": "system", "content": SYSTEM_PROMPT}])
#                 history.append({"role": "user", "content": english_text})
#                 interrupted = False

#                 async def stream_response():
#                     nonlocal interrupted, history
#                     reply_en = ""
#                     try:
#                         stream = await openrouter_client.chat.completions.create(
#                             model=MODEL,
#                             messages=history,
#                             stream=True
#                         )
#                         async for chunk in stream:
#                             if interrupted:
#                                 break
#                             delta = chunk.choices[0].delta.content or ""
#                             if delta:
#                                 reply_en += delta
#                                 if lang_spitch != "en":
#                                     try:
#                                         partial_local = spitch_translate(delta, source="en", target=lang_spitch)
#                                     except Exception as e:
#                                         logger.error(f"Translation error (output): {e}")
#                                         partial_local = delta
#                                 else:
#                                     partial_local = delta

#                                 voice_for_lang = SPITCH_VOICE_MAP.get(lang_spitch, SPITCH_VOICE_MAP["en"])

#                                 await websocket.send_text(
#                                     json.dumps({
#                                         "type": "text",
#                                         "token": partial_local,
#                                         "last": False,
#                                         "interruptible": True
#                                     })
#                                 )

#                         if not interrupted:
#                             # signal done
#                             await websocket.send_text(
#                                 json.dumps({
#                                     "type": "audio",
#                                     "audio": "",
#                                     "last": True
#                                 })
#                             )
#                             CONVERSATION_HISTORY[call_sid] = history[-20:]
#                     except Exception as e:
#                         logger.error(f"Error in stream_response: {e}")
#                         if not interrupted:
#                             await websocket.send_text(
#                                 json.dumps({
#                                     "type": "audio",
#                                     "audio": "",
#                                     "last": True
#                                 })
#                             )

#                 current_response_task = asyncio.create_task(stream_response())

#             elif event_type == "speaker":
#                 if message.get("event") == "clientSpeaking":
#                     interrupted = True
#                 continue

#             elif event_type == "call_ended":
#                 LANGUAGE_SELECTION.pop(call_sid, None)
#                 CONVERSATION_HISTORY.pop(call_sid, None)
#                 continue

#     except Exception as e:
#         logger.error(f"WebSocket error: {e}")
#     finally:
#         if current_response_task:
#             current_response_task.cancel()
#         receive_task.cancel()
#         if call_sid:
#             LANGUAGE_SELECTION.pop(call_sid, None)
#             CONVERSATION_HISTORY.pop(call_sid, None)
#         try:
#             if websocket.application_state == WebSocketState.CONNECTED:
#                 await websocket.close()
#         except Exception:
#             pass