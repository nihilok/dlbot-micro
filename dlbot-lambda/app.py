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
    # Check ffmpeg version
    ffmpeg_version = subprocess.check_output(["ffmpeg", "-version"])
    logger.info(f"ffmpeg version: {ffmpeg_version.decode('utf-8')}")

    # Extract the URL and chat_id from the SNS message
    message = event["Records"][0]["Sns"]["Message"]
    chat_id, message_id, url = message.split("::")

    # Download file(s) using yt-dlp
    files = download_url(url)  # Single file unless playlist url
    for file in files:
        s3_key = file.filename.replace("/tmp/", f"{chat_id}/")
        # Save the content to S3
        with open(file.filename, "rb") as f:
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=f.read())

        # Notify the Telegram bot that the download is complete
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Message=f"{message_id}::{s3_key}",
            Subject="Download Complete",
            MessageGroupId=chat_id,
        )

    return {
        "statusCode": 200,
        "body": f"{len(files)} file(s) downloaded and stored in S3.",
    }
