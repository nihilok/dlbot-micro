import asyncio
import logging
import os

from telegram import Bot

from boto3_clients import s3_client
from constants import S3_BUCKET
from lib import (
    download_url,
    update_placeholder_audio_message,
)
from yt_downloader_cache import S3PersistentCache

SNS_TOPIC = os.environ["SNS_TOPIC"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MAX_FILE_SIZE = int(50e6)  # 50MB

logger = logging.getLogger(__name__)


def get_message_attrs(chat_id, message_id, placeholder_id=None, url=None):
    attrs = {
        "message_id": {
            "DataType": "String",
            "StringValue": str(message_id),
        },
        "chat_id": {"DataType": "String", "StringValue": str(chat_id)},
    }
    if placeholder_id:
        attrs["placeholder_id"] = {
            "DataType": "String",
            "StringValue": str(placeholder_id),
        }
    if url:
        attrs["url"] = {"DataType": "String", "StringValue": url}
    return attrs


def lambda_handler(event, _):
    # Extract the URL and chat_id/message_id from the SNS message/attributes
    loop = asyncio.new_event_loop()
    bot = Bot(token=BOT_TOKEN)
    exceptions_raised = False

    for queued_message in event["Records"]:
        video_url = queued_message["body"]
        attributes = queued_message["messageAttributes"]
        chat_id = int(attributes["chat_id"]["stringValue"])
        placeholder_message_id = int(attributes["placeholder_audio_id"]["stringValue"])

        # Check whether file(s) already exist, it's possible the send operation failed,
        # but the download was completed successfully; or we just still have a cached version.
        prefix = f"downloads/{hash(video_url)}/"
        existing = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        tasks = []
        if "Contents" not in existing:
            # Download file(s) using yt-dlp
            for file in download_url(
                video_url,
                chat_id,
                cache_cls=S3PersistentCache,
            ):  # Yields a single file unless URL is for a playlist
                file_size = os.path.getsize(file.filename)
                if file_size >= MAX_FILE_SIZE:
                    continue

                # Save the content to S3
                s3_key = file.filename.replace("/tmp/", prefix)

                with open(file.filename, "rb") as f:
                    audio_bytes = f.read()
                    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=audio_bytes)
                    task = loop.create_task(
                        update_placeholder_audio_message(
                            chat_id, placeholder_message_id, audio_bytes, bot
                        )
                    )
                    tasks.append(task)
        else:
            for obj in existing["Contents"]:
                audio_bytes = obj["Body"].read()
                task = loop.create_task(
                    update_placeholder_audio_message(
                        chat_id, placeholder_message_id, audio_bytes, bot
                    )
                )
                tasks.append(task)

            results = asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(result, exc_info=True)
                    exceptions_raised = True

        return {"statusCode": 200} if not exceptions_raised else {"statusCode": 400}
