import asyncio
import os
import subprocess

import boto3
from telegram import Bot
from telegram.error import TimedOut

BOT_TOKEN = os.environ["DLBOT_TOKEN"]
BUCKET_NAME = os.environ["BUCKET_NAME"]


# def get_new_files(reverse_order=False):
#     search_dir = "./"
#     files = filter(os.path.isfile, os.listdir(search_dir))
#     files = [os.path.join(search_dir, f) for f in files]  # add path to each file
#     files.sort(key=lambda x: os.path.getmtime(x), reverse=reverse_order)
#     for file in files:
#         if "s__" in file and ".mp3" in file:
#             yield file
#
#
# def split_large_file(filepath):
#     subprocess.call(["mp3splt", filepath, "-t", "30.0.0"])


async def edit_message_ignore_errors(bot, text, chat_id, message_id):
    try:
        await bot.edit_message_text(text, chat_id, message_id)
    except Exception:
        pass


async def delete_message_ignore_errors(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def do_the_thing(s3_key, message_id):
    bot = Bot(token=BOT_TOKEN)
    chat_id, *_ = s3_key.split("/")
    await edit_message_ignore_errors(bot, "Sending audio...", chat_id, message_id)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    data = obj["Body"].read()
    try:
        await send_audio(bot, chat_id, data)
    except Exception as e:
        await send_error_message(
            chat_id, message_id, f"😭Something went wrong sending audio\n{e}"
        )
        return
    s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
    await delete_message_ignore_errors(bot, chat_id, message_id)


async def send_error_message(chat_id, message_id, error_message):
    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.edit_message_text(error_message, chat_id, message_id)
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
    except (KeyError, ValueError):
        return {"statusCode": 400}

    attributes = event["Records"][0]["Sns"]["MessageAttributes"]

    try:
        chat_id = int(attributes["chat_id"]["Value"])
        message_id = int(attributes["message_id"]["Value"])
    except Exception as e:
        return {
            "statusCode": 400,
            "error": {"class": e.__class__.__name__, "text": str(e)},
        }

    if (url := attributes.get("url")) is not None:
        error = message
        asyncio.run(
            send_error_message(
                chat_id,
                message_id,
                f"😭Sending mp3 from {url} failed\n({error})",
            )
        )
    else:
        s3_key = message
        asyncio.run(do_the_thing(s3_key, message_id))

    return {"statusCode": 200}
