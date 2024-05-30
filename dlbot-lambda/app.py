import logging
import os

import boto3

from lib import delete_message_blocking, download_url, send_message_blocking

SNS_TOPIC = os.environ["SNS_TOPIC"]
S3_BUCKET = os.environ["S3_BUCKET"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MAX_FILE_SIZE = int(50e6)  # 50MB

sns_client = boto3.client("sns", region_name="eu-west-2")
s3_client = boto3.client("s3")

logger = logging.getLogger(__name__)


def get_message_attrs(chat_id, message_id, url=None):
    attrs = {
        "message_id": {
            "DataType": "String",
            "StringValue": str(message_id),
        },
        "chat_id": {"DataType": "String", "StringValue": str(chat_id)},
    }
    if url:
        attrs["url"] = {"DataType": "String", "StringValue": url}
    return attrs


def lambda_handler(event, _):
    # Extract the URL and chat_id/message_id from the SNS message/attributes
    message = None
    try:
        message = event["Records"][0]["Sns"]["Message"]
        attributes = event["Records"][0]["Sns"]["MessageAttributes"]
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

    # Check whether file(s) already exist, it's possible the send operation failed,
    # but the download was completed successfully.
    prefix = f"{chat_id}/{hash(message)}/"
    existing = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in existing:
        # Download file(s) using yt-dlp
        url = message
        delete_message_blocking(chat_id, message_id)
        message_id = send_message_blocking(chat_id, "Downloading...")
        for file in download_url(
            url, chat_id, message_id
        ):  # Yields a single file unless URL is for a playlist
            file_size = os.path.getsize(file.filename)
            if file_size >= MAX_FILE_SIZE:
                sns_client.publish(
                    TopicArn=SNS_TOPIC,
                    Message="File size too large",
                    MessageAttributes=get_message_attrs(chat_id, message_id, url),
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
                MessageAttributes=get_message_attrs(chat_id, message_id),
            )
    else:
        for obj in existing["Contents"]:
            # Notify that the download is complete
            sns_client.publish(
                TopicArn=SNS_TOPIC,
                Message=obj["Key"],
                MessageAttributes=get_message_attrs(chat_id, message_id),
            )
    return {"statusCode": 200}
