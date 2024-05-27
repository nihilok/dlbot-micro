import asyncio
import logging
import os
import subprocess

import boto3

from lib import download_url

SNS_TOPIC = os.environ["SNS_TOPIC"]
S3_BUCKET = os.environ["S3_BUCKET"]

sns_client = boto3.client("sns", region_name="eu-west-2")
s3_client = boto3.client("s3")


logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    # Extract the URL and chat_id from the SNS message
    try:
        message = event["Records"][0]["Sns"]["Message"]
        chat_id, message_id, url = message.split("::")
    except (KeyError, ValueError):
        return {"statusCode": 400}

    # Download file(s) using yt-dlp
    files = download_url(url)
    for file in files:  # Single file unless playlist url
        # Save the content to S3
        s3_key = file.filename.replace("/tmp/", f"{chat_id}/")
        with open(file.filename, "rb") as f:
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=f.read())

        asyncio.run(bot.delete_message(chat_id, message_id))
        asyncio.run(bot.send_message(chat_id, f"S3 Object Key: {s3_key}"))

        # Notify the Telegram bot that the download is complete
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Message=f"{message_id}::{s3_key}",
        )

    return {
        "statusCode": 200,
        "body": f"{len(files)} file(s) downloaded and stored in S3.",
    }
