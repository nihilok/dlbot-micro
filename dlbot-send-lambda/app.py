import asyncio
import os
import subprocess

import boto3
from telegram import Bot
from telegram.error import TimedOut

BOT_TOKEN = os.environ["DLBOT_TOKEN"]
BUCKET_NAME = os.environ["BUCKET_NAME"]


def get_new_files(reverse_order=False):
    search_dir = "./"
    files = filter(os.path.isfile, os.listdir(search_dir))
    files = [os.path.join(search_dir, f) for f in files]  # add path to each file
    files.sort(key=lambda x: os.path.getmtime(x), reverse=reverse_order)
    for file in files:
        if "s__" in file and ".mp3" in file:
            yield file


def split_large_file(filepath):
    subprocess.call(["mp3splt", filepath, "-t", "30.0.0"])


async def do_the_thing(s3_key, message_id):
    bot = Bot(token=BOT_TOKEN)
    chat_id, *_ = s3_key.split("/")
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    data = obj["Body"].read()
    await send_audio(bot, chat_id, data)
    s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)


async def send_error_message(chat_id, message_id, error_message):
    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.edit_message_text(chat_id, message_id, error_message)
    except Exception:
        await bot.send_message(chat_id, error_message)


async def send_audio(bot, chat_id, data):
    try:
        await bot.send_audio(chat_id, data)
    except TimedOut:
        # Was most likely successful
        pass


def lambda_handler(event, _):
    del _
    try:
        message = event["Records"][0]["Sns"]["Message"]
        message_id, *remainder = message.split("::")
        del message
        del event
    except (KeyError, ValueError):
        return {"statusCode": 400}

    if len(remainder) == 3:
        chat_id, error_message, url = remainder
        asyncio.run(
            send_error_message(
                chat_id, message_id, f"Failed to download {url}\n({error_message})"
            )
        )
    elif len(remainder) == 1:
        s3_key = remainder
    else:
        return {"statusCode": 400}

    asyncio.run(do_the_thing(s3_key, message_id))
    return {"statusCode": 200}
