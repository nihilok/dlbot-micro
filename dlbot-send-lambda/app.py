import asyncio
import logging
import os
import time
from random import randint

import boto3
from telegram import Bot, InputMediaAudio
from telegram.error import TimedOut

BOT_TOKEN = os.environ["DLBOT_TOKEN"]
BUCKET_NAME = os.environ["BUCKET_NAME"]


logger = logging.getLogger(__name__)


async def edit_message_ignore_errors(bot, text, chat_id, message_id):
    try:
        await bot.edit_message_text(text, chat_id, message_id)
    except Exception as e:
        logger.warning(str(e), exc_info=True)


async def delete_message_ignore_errors(chat_id, message_id):
    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        logger.warning(str(e), exc_info=True)


async def add_audio(bot: Bot, chat_id, data, message_id):
    tg_audio = InputMediaAudio(data)
    await bot.edit_message_media(tg_audio, chat_id, message_id)


MAX_RETRIES = 5


async def do_the_thing(s3_key, message_id, placeholder_id, retry=0):
    await asyncio.sleep(randint(1, 5))
    try:
        bot = Bot(token=BOT_TOKEN)
        chat_id, *_ = s3_key.split("/")
        await edit_message_ignore_errors(bot, "Sending audio...", chat_id, message_id)
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        data = obj["Body"].read()
        await add_audio(bot, chat_id, data, placeholder_id)
    except Exception as e:
        if retry < MAX_RETRIES:
            logger.warning(f"Retrying ({retry + 1}/{MAX_RETRIES}) (ERROR: {e})")
            return await do_the_thing(
                s3_key, message_id, placeholder_id, retry=retry + 1
            )
        raise e

    s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
    await delete_message_ignore_errors(chat_id, message_id)


async def send_error_message(chat_id, message_id, error_message):
    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.edit_message_text(error_message, chat_id, message_id)
    except Exception as e:
        logger.warning(f"Cannot edit placeholder message ({e})", exc_info=True)
        await bot.send_message(chat_id, error_message)


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
        placeholder_id = int(attributes["placeholder_id"]["Value"])
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
        asyncio.run(do_the_thing(s3_key, message_id, placeholder_id))

    return {"statusCode": 200}
