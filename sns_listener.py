import asyncio
import os

import boto3
from flask import Flask, jsonify, request
from telegram import Bot

BOT_TOKEN = os.getenv("DLBOT_TOKEN")

app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)


@app.route("sns", methods=["POST"])
def sns():
    message_type = request.headers.get("x-amz-sns-message-type")
    if message_type == "SubscriptionConfirmation":
        # Confirm the subscription
        sns_message = request.get_json()
        token = sns_message["Token"]
        topic_arn = sns_message["TopicArn"]
        sns_client = boto3.client("sns")
        sns_client.confirm_subscription(TopicArn=topic_arn, Token=token)
        return jsonify({"message": "Subscription confirmed"}), 200
    elif message_type == "Notification":
        # Process the notification
        # Must be in the format: `<message_id>::<chat_id>/<filename>`
        sns_message = request.get_json()
        message_id, s3_key = sns_message.split("::")
        chat_id, _filename = s3_key.split("/")
        asyncio.run(bot.delete_message(chat_id, message_id))
        asyncio.run(bot.send_message(chat_id, f"S3 Object Key: {s3_key}"))
        return jsonify({"message": "Notification received"}), 200
    else:
        return jsonify({"message": "Unknown message type"}), 400


if __name__ == "__main__":
    app.run("0.0.0.0", 9999, debug=False)
