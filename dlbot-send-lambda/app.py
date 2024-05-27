import asyncio
import os

import boto3
from telegram import Bot
from telegram.error import TimedOut

BOT_TOKEN = os.environ["DLBOT_TOKEN"]
BUCKET_NAME = os.environ["BUCKET_NAME"]


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
    try:
        await bot.send_audio(chat_id, data)
    except TimedOut:
        # Was most likely successful
        pass


def lambda_handler(event, _):
    del _
    try:
        message = event["Records"][0]["Sns"]["Message"]
        message_id, s3_key = message.split("::")
        del message
        del event
    except (KeyError, ValueError):
        return {"statusCode": 400}

    asyncio.run(do_the_thing(s3_key, message_id))
    return {"statusCode": 200}
