import asyncio
import io
import logging
import os
from random import randint
from typing import NamedTuple, Type

import requests
import yt_dlp
from mutagen.easyid3 import EasyID3
from telegram import Bot, InputMediaAudio
from telegram.error import RetryAfter
from yt_dlp.cache import Cache

from yt_downloader_cache import S3PersistentCache
from constants import MAX_AUDIO_UPDATE_RETRIES

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
BOT_TOKEN = os.environ["BOT_TOKEN"]


def send_message_blocking(chat_id, text) -> int:
    r = requests.get(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={chat_id}&text={text}"
    )
    if r.ok:
        return r.json()["result"]["message_id"]


DOWNLOAD_OPTIONS = {
    "paths": {"home": "/tmp/", "temp": "/tmp/"},
    "outtmpl": "%(id)s.%(ext)s",
    "format": "bestaudio/best",
    "cachedir": False,
    "logtostderr": True,
    "postprocessors": [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
        }
    ],
}


class File(NamedTuple):
    filename: str
    artist: str
    title: str
    url: str


class Downloader(yt_dlp.YoutubeDL):
    def __init__(self, options, cache_cls: Type[Cache] = S3PersistentCache):
        options["username"] = "oauth2"
        options["password"] = ""
        super().__init__(options)
        self.cache = cache_cls(self)


def parse_metadata(result):
    artist = result.get("artist", None)
    if artist:
        artists = artist.split(", ")
        artist = ", ".join(sorted(set(artists), key=lambda x: artists.index(x)))
    title = result.get("title") or result.get("alt_title")
    try:
        if artist is None and " - " in title:
            artist = title.split(" - ")[0]
            title = title.split(" - ")[-1]
    except IndexError:
        artist = None
        title = result.get("title") or result.get("alt_title")
    logger.info(f"Returning: {artist}, {title}")
    return artist, title


def set_tags(filepath, title, artist=None):
    try:
        metatag = EasyID3(filepath)
        metatag["title"] = title
        if artist is not None:
            metatag["artist"] = artist
        metatag.save()
    except Exception as e:
        logger.error(f"Error settings tags: {e}")


def get_opts():
    opts = DOWNLOAD_OPTIONS.copy()
    return opts


def download_single_url(url, cache_cls=Cache):
    opts = get_opts()
    with Downloader(opts, cache_cls) as ydl:
        result = ydl.extract_info(url, download=True)
        if "entries" in result:
            info = result["entries"][0]
        else:
            info = result
        artist, title = parse_metadata(info)
        filename = f"/tmp/{info['id']}.mp3"
        if not os.path.exists(filename):
            raise FileNotFoundError
        set_tags(filename, title, artist)
        return File(filename, artist, title, url), 0


def download_playlist(url, chat_id=None, cache_cls=Cache):
    """Download each video in the playlist and return the information as a list of tuples"""
    with Downloader({"extract_flat": True}, cache_cls) as flat:
        info = flat.extract_info(url, download=False)
        title = info["title"]
        count = info["playlist_count"]
        send_message_blocking(chat_id, f"{title} ({count} tracks)")
    for entry in info["entries"]:
        result, exit_code = download_single_url(entry["url"], cache_cls)
        if not exit_code:
            yield result


def download_url(url: str, chat_id=None, cache_cls=Cache):
    if "playlist" in url:
        return download_playlist(url, chat_id, cache_cls)
    else:
        file, exit_code = download_single_url(url, cache_cls)
        if not exit_code:
            return (f for f in [file])
        raise Exception(f"Could not download from URL: {url}")


async def update_placeholder_audio_message(
    chat_id, message_id, audio_bytes, bot: Bot, retry=0
):
    tg_audio = InputMediaAudio(audio_bytes)
    try:
        await bot.edit_message_media(tg_audio, chat_id, message_id)
    except Exception as e:
        if isinstance(e, RetryAfter):
            sleep_time = e.retry_after
        else:
            sleep_time = randint(3, 10)

        if retry < MAX_AUDIO_UPDATE_RETRIES:
            await asyncio.sleep(sleep_time)
            logger.warning(
                f"Retrying ({retry + 1}/{MAX_AUDIO_UPDATE_RETRIES}) (ERROR: {e})"
            )
            return await update_placeholder_audio_message(
                chat_id, message_id, audio_bytes, bot, retry=retry + 1
            )

        raise e


if __name__ == "__main__":
    print(download_url("https://www.youtube.com/watch?v=bgWUwywrXOM"))
