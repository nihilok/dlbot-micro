import os
import re

import boto3
from telegram.ext import (Application, ApplicationBuilder, MessageHandler,
                          filters)

SNS_TOPIC = os.environ["SNS_POST_TOPIC"]
BOT_TOKEN = os.environ["DLBOT_TOKEN"]
TABLE_NAME = os.environ["DDB_TABLE_NAME"]

session = boto3.Session(profile_name="LambdaFlowFullAccess")
sns_client = session.client("sns", region_name="eu-west-2")
dynamodb = session.resource("dynamodb", region_name="eu-west-2")
table = dynamodb.Table(TABLE_NAME)


class NotAuthenticated(ValueError):
    pass


def authenticate(user_id, chat_id):
    chat_response = table.get_item(Key={"id": chat_id})
    user_response = table.get_item(Key={"id": user_id})
    if "Item" not in chat_response:
        if "Item" not in user_response:
            raise NotAuthenticated
    return True


def parse_message_for_urls(message):
    urls = re.findall(r"https://\S+", message)
    for url in urls:
        yield url


async def message_handler(update, context):
    authenticate(update.message.from_user.id, update.effective_chat.id)
    for url in parse_message_for_urls(update.message.text):
        message_id = await context.bot.send_message(
            update.effective_chat.id, "Downloading..."
        )
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Message=f"{update.effective_chat.id}::{message_id.id}::{url}",
        )


def build_bot(token: str) -> Application:
    application = ApplicationBuilder().token(token).build()
    application.add_handler(MessageHandler(filters.TEXT, message_handler))
    return application


def run_polling(application: Application):
    application.run_polling()


if __name__ == "__main__":
    run_polling(build_bot(BOT_TOKEN))
