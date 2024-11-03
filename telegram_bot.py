import asyncio
import io
import os
import re
import time
import wave
from uuid import uuid4

import boto3
import yt_dlp
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    MessageHandler,
    filters,
    ContextTypes,
)

SQS_QUEUE = os.environ["SQS_QUEUE"]
BOT_TOKEN = os.environ["DLBOT_TOKEN"]
TABLE_NAME = os.environ["DDB_TABLE_NAME"]

session = boto3.Session(profile_name="LambdaFlowFullAccess")
sqs_client = session.client("sqs", region_name="eu-west-2")
dynamodb = session.resource("dynamodb", region_name="eu-west-2")
table = dynamodb.Table(TABLE_NAME)


class NotAuthenticated(ValueError):
    pass


def create_dummy_audio():
    # Parameters for the dummy audio
    nchannels = 1
    sampwidth = 2
    framerate = 44100
    nframes = framerate  # 1 second of audio
    comptype = "NONE"
    compname = "not compressed"

    # Create a buffer to hold the audio data
    buffer = io.BytesIO()

    # Create a wave file
    with wave.open(buffer, "wb") as wf:
        wf.setnchannels(nchannels)
        wf.setsampwidth(sampwidth)
        wf.setframerate(framerate)
        wf.setnframes(nframes)
        wf.setcomptype(comptype, compname)
        # Generate silent audio (all zeros)
        wf.writeframes(b"\x00" * nframes * sampwidth * nchannels)

    buffer.seek(0)
    return buffer


async def send_dummy_audio_message(chat_id, context: ContextTypes.DEFAULT_TYPE) -> int:
    audio = create_dummy_audio()
    message = await context.bot.send_audio(chat_id, audio, title="Downloading...")
    return message.id


def authenticate(user_id, chat_id):
    chat_response = table.get_item(Key={"id": chat_id})
    if "Item" not in chat_response:
        user_response = table.get_item(Key={"id": user_id})
        if "Item" not in user_response:
            raise NotAuthenticated
    return True


def parse_message_for_urls(message):
    urls = re.findall(r"https://\S+", message)
    for url in urls:
        yield url


async def playlist_info(url, bot, chat_id):
    with yt_dlp.YoutubeDL({"extract_flat": True}) as flat:
        info = flat.extract_info(url, download=False)
        title = info["title"]
        count = info["playlist_count"]
        message = await bot.send_message(chat_id, f"{title} ({count} tracks)")
        print(message.id)
        for entry in info["entries"]:
            yield entry["url"]


async def message_handler(update, context: ContextTypes.DEFAULT_TYPE):
    authenticate(update.message.from_user.id, update.effective_chat.id)
    queue_url = sqs_client.get_queue_url(QueueName=SQS_QUEUE)["QueueUrl"]
    for url in parse_message_for_urls(update.message.text):
        message_attrs = {
            "chat_id": {
                "DataType": "String",
                "StringValue": str(update.effective_chat.id),
            }
        }
        message_group_id = f"{update.effective_chat.id}-{url}"
        try:
            if "playlist" in url:
                async for playlist_entry_url in playlist_info(
                    url, context.bot, update.effective_chat.id
                ):
                    await queue_single_url(
                        update,
                        context,
                        message_attrs,
                        message_group_id,
                        playlist_entry_url,
                        queue_url,
                    )
                    await asyncio.sleep(0.5)
            else:
                await queue_single_url(
                    update, context, message_attrs, message_group_id, url, queue_url
                )
        except Exception as e:
            await context.bot.send_message(
                update.effective_chat.id,
                f"*Something went wrong* 😢\n{url}\n{e}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )


async def queue_single_url(
    update, context, message_attrs, message_group_id, audio_url, queue_url
):
    placeholder_audio_id = await send_dummy_audio_message(
        update.effective_chat.id, context
    )
    current_message = message_attrs.copy()
    current_message["placeholder_audio_id"] = {
        "DataType": "String",
        "StringValue": str(placeholder_audio_id),
    }
    message_deduplication_id = str(uuid4())
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=audio_url,
        MessageAttributes=current_message,
        MessageGroupId=message_group_id,
        MessageDeduplicationId=message_deduplication_id,
    )


def build_bot(token: str) -> Application:
    application = ApplicationBuilder().token(token).build()
    application.add_handler(MessageHandler(filters.TEXT, message_handler))
    return application


def run_polling(application: Application):
    application.run_polling()


if __name__ == "__main__":
    run_polling(build_bot(BOT_TOKEN))
