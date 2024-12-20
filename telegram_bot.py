import asyncio
import io
import logging
import os
import re
import wave
from random import randint
from uuid import uuid4

import aiohttp

import boto3
import yt_dlp
from telegram import helpers, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import RetryAfter, TimedOut
from telegram.ext import (
    Application,
    ApplicationBuilder,
    MessageHandler,
    filters,
    ContextTypes,
    CommandHandler,
    ChatMemberHandler,
)

SQS_QUEUE = os.environ["SQS_QUEUE"]
USE_SQS = os.environ.get("USE_SQS", "false").lower() == "true"
SNS_TOPIC = os.environ["SNS_POST_TOPIC"]
BOT_TOKEN = os.environ["DLBOT_TOKEN"]
DEBUG_BOT_TOKEN = os.environ.get("DLBOT_TOKEN_DEBUG")
DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
MEMBERS_CHANNEL_ID = os.environ["MEMBERS_CHANNEL_ID"]
MEMBERS_CHANNEL_LINK = os.environ["MEMBERS_CHANNEL_LINK"]
NEW_USERS_TABLE = os.environ["NEW_USERS_TABLE"]
ERRORS_TABLE = os.environ["ERRORS_TABLE"]
MAXIMUM_PLAYLIST_LENGTH = int(os.environ.get("MAXIMUM_PLAYLIST_LENGTH", 30))

MAX_RETRIES_FOR_SENDING_PLACEHOLDER_MESSAGE = 5

session = boto3.Session(profile_name="LambdaFlowFullAccess")
sqs_client = session.client("sqs", region_name="eu-west-2")
sns_client = session.client("sns", region_name="eu-west-2")
dynamodb = session.resource("dynamodb", region_name="eu-west-2")
new_users_table = dynamodb.Table(NEW_USERS_TABLE)
errors_table = dynamodb.Table(ERRORS_TABLE)
queue_url = sqs_client.get_queue_url(QueueName=SQS_QUEUE)["QueueUrl"]

if DEBUG:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)


async def download_image(url: str) -> bytes:
    async with aiohttp.ClientSession() as request:
        async with request.get(url) as response:
            if response.status == 200:
                return await response.read()


def create_dummy_audio():
    # Parameters for the dummy audio
    nchannels = 1
    sampwidth = 2
    framerate = 44100
    nframes = framerate  # 1 second of audio
    comptype = "NONE"
    compname = "not compressed"

    # Create a buffer to hold the audio data
    buffer = io.BytesIO()

    # Create a wave file
    with wave.open(buffer, "wb") as wf:
        wf.setnchannels(nchannels)
        wf.setsampwidth(sampwidth)
        wf.setframerate(framerate)
        wf.setnframes(nframes)
        wf.setcomptype(comptype, compname)
        # Generate silent audio (all zeros)
        wf.writeframes(b"\x00" * nframes * sampwidth * nchannels)

    buffer.seek(0)
    return buffer


async def send_dummy_audio_message(
    chat_id, context: ContextTypes.DEFAULT_TYPE, retry=0
) -> int:
    audio = create_dummy_audio()
    try:
        message = await context.bot.send_audio(chat_id, audio, title="Downloading...")
        return message.id

    except TimedOut:
        pass
    except Exception as e:
        if isinstance(e, RetryAfter):
            sleep_time = e.retry_after
        else:
            sleep_time = randint(3, 10)
        if retry < MAX_RETRIES_FOR_SENDING_PLACEHOLDER_MESSAGE:
            await asyncio.sleep(sleep_time)
            return await send_dummy_audio_message(chat_id, context, retry=retry + 1)
        raise e


async def check_membership(update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = MEMBERS_CHANNEL_ID
    user_id = update.effective_user.id
    chat_member = await context.bot.get_chat_member(chat_id, user_id)
    member = chat_member.status in ["creator", "administrator", "member"]
    admin = chat_member.status in ["creator", "administrator"]
    return member, admin


def parse_message_for_urls(message):
    urls = re.findall(r"https://\S+", message)
    for url in urls:
        yield url


async def playlist_info(url, bot, chat_id, max_tracks=None):
    with yt_dlp.YoutubeDL({"extract_flat": True}) as flat:
        info = flat.extract_info(url, download=False)
        title = info["title"]
        count = info["playlist_count"]
        release_year = info.get("release_year")
        if max_tracks and count > max_tracks:
            await bot.send_message(
                chat_id,
                f"Sorry, I can't download playlists with more than {max_tracks} tracks.",
            )
        message = helpers.escape_markdown(
            f"{title} ({count} tracks){' (' + release_year + ')' if release_year else ''}"
        )
        try:
            if info.get("thumbnails"):
                try:
                    image_url = info["thumbnails"][-2]["url"]
                except IndexError:
                    image_url = info["thumbnails"][0]["url"]
                image_content = await download_image(image_url)
                await bot.send_photo(chat_id, image_content, caption=message)
        except Exception:
            await bot.send_message(chat_id, message)

        for entry in info["entries"]:
            yield entry["url"]


def save_init_message_data(user_id, message_id):
    new_users_table.put_item(Item={"user_id": user_id, "message_id": message_id})


def get_init_message_data(user_id):
    row = new_users_table.get_item(Key={"user_id": user_id})
    return row["Item"] if "Item" in row else None


async def queue_single_url(
    update, context, message_attrs, message_group_id, audio_url, queue_url
):
    placeholder_audio_id = await send_dummy_audio_message(
        update.effective_chat.id, context
    )
    current_message = message_attrs.copy()
    current_message["placeholder_audio_id"] = {
        "DataType": "String",
        "StringValue": str(placeholder_audio_id),
    }
    if not USE_SQS:
        sns_client.publish(
            TopicArn=SNS_TOPIC, Message=audio_url, MessageAttributes=current_message
        )
    else:
        message_deduplication_id = str(uuid4())
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=audio_url,
            MessageAttributes=current_message,
            MessageGroupId=message_group_id,
            MessageDeduplicationId=message_deduplication_id,
        )


async def member_join_handler(update, context: ContextTypes.DEFAULT_TYPE):
    new_chat_member = update.chat_member.new_chat_member
    if new_chat_member.status != new_chat_member.MEMBER:
        return
    user_id = new_chat_member.user.id
    data = get_init_message_data(user_id)
    if not data:
        return

    message_id = int(data["message_id"])
    success_button = InlineKeyboardButton(
        text="Channel Joined ✅", url=MEMBERS_CHANNEL_LINK
    )
    await context.bot.edit_message_text(
        text=f"Congratulations! 🎉 You are now a member! Send me a link to a YouTube video/playlist, and I'll send you the MP3(s)! 🎵🎧",
        chat_id=user_id,
        message_id=message_id,
        reply_markup=InlineKeyboardMarkup([[success_button]]),
    )
    new_users_table.delete_item(Key={"user_id": user_id})


async def instructions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        update.effective_chat.id,
        "Send me a link to a YouTube video or playlist, and I'll send you the MP3(s)! "
        "Messages may contain multiple URLs. Messages without URLs (that are not commands "
        "e.g. /start) will be ignored. There is a maximum file size of 50MB, and a maximum "
        f"playlist length of {MAXIMUM_PLAYLIST_LENGTH} tracks. ",
    )


async def retry_all_failures(update: Update, context: ContextTypes.DEFAULT_TYPE):
    response = errors_table.query(
        dynamodb.conditions.Key("chat_id").eq(update.effective_chat.id)
    )
    for item in response["Items"]:
        video_url = item["video_url"]
        message_id = item["message_id"]
        message_attrs = {
            "chat_id": {
                "DataType": "String",
                "StringValue": str(update.effective_chat.id),
            },
            "placeholder_audio_id": {
                "DataType": "String",
                "StringValue": str(message_id),
            },
        }
        try:
            sns_client.publish(
                TopicArn=SNS_TOPIC, Message=video_url, MessageAttributes=message_attrs
            )
            errors_table.delete_item(
                Key={"chat_id": update.effective_chat.id, "message_id": message_id}
            )
        except Exception as e:
            logger.error(f"Failed to retry message {message_id}: {e}")
            await context.bot.send_message(
                update.effective_chat.id, f"Failed to retry {video_url}: {e}"
            )
        await asyncio.sleep(2)


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    member, admin = await check_membership(update, context)
    is_own_chat = update.effective_chat.id == update.effective_user.id
    if not admin and not is_own_chat:
        bot_username = (await context.bot.get_me()).username
        await update.message.reply_text(
            f"Sorry, {update.effective_user.first_name}, you are not permitted use this bot in groups.\n@{bot_username} <- click here to open a private chat.",
        )
        return

    if not member:
        join_button = InlineKeyboardButton(
            text="Join Channel", url=MEMBERS_CHANNEL_LINK
        )
        keyboard = InlineKeyboardMarkup([[join_button]])
        message = await update.message.reply_text(
            "You must be a member to use this bot. Click the button to join the members channel. (By joining the channel you will be automatically allowed to use the bot.)",
            reply_markup=keyboard,
        )
        save_init_message_data(update.effective_user.id, message.message_id)
        return

    if update.message.text == "/start":
        message_prefix = f"You're already {'an admin' if admin else 'a member'}! "
        message = "Send me a link and I'll send you the MP3!"
        if is_own_chat:
            message = message_prefix + message
        await context.bot.send_message(
            update.effective_chat.id,
            message,
        )
        return

    for url in parse_message_for_urls(update.message.text):
        if "spotify" in url:
            await context.bot.send_message(
                update.effective_chat.id,
                "Sorry, I can't download from Spotify 😢",
            )
            continue
        message_attrs = {
            "chat_id": {
                "DataType": "String",
                "StringValue": str(update.effective_chat.id),
            }
        }
        message_group_id = f"{update.effective_chat.id}-{url}"
        try:
            if "playlist" in url:
                async for playlist_entry_url in playlist_info(
                    url,
                    context.bot,
                    update.effective_chat.id,
                    max_tracks=MAXIMUM_PLAYLIST_LENGTH,
                ):
                    await queue_single_url(
                        update,
                        context,
                        message_attrs,
                        message_group_id,
                        playlist_entry_url,
                        queue_url,
                    )
                    await asyncio.sleep(2)
            else:
                await queue_single_url(
                    update, context, message_attrs, message_group_id, url, queue_url
                )
        except Exception as e:
            error_message = helpers.escape_markdown(str(e))
            await context.bot.send_message(
                update.effective_chat.id,
                f"Something went wrong! 😢\n\n{url}\n\n{error_message}",
            )


def build_bot(token: str) -> Application:
    application = ApplicationBuilder().token(token).build()
    application.add_handler(
        ChatMemberHandler(
            member_join_handler,
            ChatMemberHandler.CHAT_MEMBER,
            chat_id=int(MEMBERS_CHANNEL_ID),
        )
    )
    application.add_handler(CommandHandler("start", message_handler))
    application.add_handler(CommandHandler("retry", retry_all_failures))
    application.add_handler(CommandHandler("help", instructions))
    url_pattern = re.compile(r"https?://\S+")
    application.add_handler(MessageHandler(filters.Regex(url_pattern), message_handler))
    return application


def run_polling(application: Application):
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    token = DEBUG_BOT_TOKEN if DEBUG else BOT_TOKEN
    run_polling(build_bot(token))
