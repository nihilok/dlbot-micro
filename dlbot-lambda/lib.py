import asyncio
import logging
import os
import time
from typing import NamedTuple

import yt_dlp
from mutagen.easyid3 import EasyID3

logger = logging.getLogger(__name__)


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


def get_metadata_local(result):
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


def get_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    return loop


def get_opts(bot, chat_id, message_id):
    async def update_message(text):
        try:
            await bot.edit_message_text(text, chat_id, message_id)
        except Exception as e:
            logger.warning(e, exc_info=True)

    def status_hook(d):
        if bot is None:
            return
        loop = get_loop()

        try:
            if d["status"] == "finished":
                loop.run_until_complete(update_message(f"Extracting MP3..."))
            elif d["status"] == "downloading":
                loop.run_until_complete(
                    update_message(
                        f"Downloading...\n{d['_percent_str']} at {d['_speed_str']} ETA: {d['_eta_str']}"
                    )
                )
        except Exception as e:
            logger.warning(e, exc_info=True)

    opts = DOWNLOAD_OPTIONS.copy()
    opts["progress_hooks"] = [status_hook]
    return opts


def download_single_url(url, bot, chat_id, message_id):
    opts = get_opts(bot, chat_id, message_id)
    with yt_dlp.YoutubeDL(opts) as ydl:
        result = ydl.extract_info(url, download=True)
        if "entries" in result:
            info = result["entries"][0]
        else:
            info = result
        artist, title = get_metadata_local(info)
        filename = f"/tmp/{info['id']}.mp3"
        if not os.path.exists(filename):
            raise FileNotFoundError
        set_tags(filename, title, artist)
        return File(filename, artist, title, url), 0


def download_playlist(url, bot, chat_id, message_id):
    """Download each video in the playlist and return the information as a list of tuples"""
    opts = get_opts(bot, chat_id, message_id)
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url)
        title = info["title"]
        count = info["playlist_count"]
        loop = get_loop()
        loop.create_task(bot.send_message(f"{title} ({count} tracks)"))
        for entry in info["entries"]:
            artist, title = get_metadata_local(entry)
            filename = f"/tmp/{entry['id']}.mp3"
            if not os.path.exists(filename):
                raise FileNotFoundError
            set_tags(filename, title, artist)
            yield File(filename, artist, title, url), 0


def download_url(url: str, bot, chat_id, message_id) -> list[File]:
    if "playlist" in url:
        files = []
        for file, exit_code in download_playlist(url, bot, chat_id, message_id):
            if not exit_code:
                files.append(file)
        return files
    else:
        file, exit_code = download_single_url(url, bot, chat_id, message_id)
        if not exit_code:
            return [file]
        raise Exception(f"Could not download from URL: {url}")


if __name__ == "__main__":
    print(
        download_url(
            "https://music.youtube.com/playlist?list=OLAK5uy_nSimGj4CXHflKeUOh_JjLOnR75Kp6Q064&si=zpprCbpnKTIc1lc6",
            None,
            None,
            None,
        )
    )
