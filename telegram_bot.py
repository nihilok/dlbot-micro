import os
import re

import boto3
from telegram.ext import (Application, ApplicationBuilder, MessageHandler,
                          filters)

SNS_TOPIC = os.getenv("SNS_POST_TOPIC")
BOT_TOKEN = os.getenv("DLBOT_TOKEN")

sns_client = boto3.client("sns", region_name="eu-west-2")


def parse_message_for_urls(message):
    urls = re.findall(r"https://\S+", message)
    for url in urls:
        yield url


async def message_handler(update, context):
    for url in parse_message_for_urls(update.message.text):
        message_id = await context.bot.send_message(
            update.effective_chat.id, "Downloading..."
        )
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Message=f"{update.effective_chat.id}::{message_id}::{url}",
            Subject="Download Complete",
            MessageGroupId=update.effective_chat.id,
        )


def build_bot(token: str) -> Application:
    application = ApplicationBuilder().token(token).build()
    application.add_handler(MessageHandler(filters.TEXT, message_handler))
    return application


def run_polling(application: Application):
    application.run_polling()


if __name__ == "__main__":
    run_polling(build_bot(BOT_TOKEN))
