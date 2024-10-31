import logging
import os

from boto3_clients import s3_client, sns_client
from constants import S3_BUCKET
from lib import (
    delete_message_blocking,
    download_url,
    send_dummy_audio_message,
    send_message_blocking,
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
    message = None
    try:
        message = event["Records"][0]["body"]
        attributes = event["Records"][0]["messageAttributes"]
        logger.info(f"Message received: {message}")
    except Exception as e:
        logger.error(f"ERROR: {e} ({message or 'no message'})")
        return {"statusCode": 400}

    try:
        chat_id = int(attributes["chat_id"]["Value"])
        message_id = int(attributes["message_id"]["Value"])
    except Exception as e:
        return {
            "statusCode": 400,
            "error": {"class": e.__class__.__name__, "text": str(e)},
        }

    delete_message_blocking(chat_id, message_id)
    message_id = send_message_blocking(chat_id, "Downloading...")
    placeholder_message_id = send_dummy_audio_message(chat_id)

    # Check whether file(s) already exist, it's possible the send operation failed,
    # but the download was completed successfully.
    prefix = f"{chat_id}/{hash(message)}/"
    existing = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in existing:
        # Download file(s) using yt-dlp
        url = message
        for file in download_url(
            url,
            chat_id,
            message_id,
            cache_cls=S3PersistentCache,
        ):  # Yields a single file unless URL is for a playlist
            file_size = os.path.getsize(file.filename)
            if file_size >= MAX_FILE_SIZE:
                sns_client.publish(
                    TopicArn=SNS_TOPIC,
                    Message="File size too large",
                    MessageAttributes=get_message_attrs(
                        chat_id,
                        message_id,
                        placeholder_id=placeholder_message_id,
                        url=url,
                    ),
                )
                continue

            # Save the content to S3
            s3_key = file.filename.replace("/tmp/", prefix)

            with open(file.filename, "rb") as f:
                s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=f.read())

            # Notify that the download is complete
            sns_client.publish(
                TopicArn=SNS_TOPIC,
                Message=s3_key,
                MessageAttributes=get_message_attrs(
                    chat_id, message_id, placeholder_message_id
                ),
            )
    else:
        for obj in existing["Contents"]:
            # Notify that the download is complete
            sns_client.publish(
                TopicArn=SNS_TOPIC,
                Message=obj["Key"],
                MessageAttributes=get_message_attrs(
                    chat_id, message_id, placeholder_message_id
                ),
            )
    return {"statusCode": 200}
