import logging
import os
from typing import NamedTuple

import requests
import yt_dlp
from mutagen.easyid3 import EasyID3

logger = logging.getLogger(__name__)
BOT_TOKEN = os.environ["BOT_TOKEN"]


def update_message_blocking(new_text, chat_id, message_id) -> int:
    r = requests.get(
        f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText?chat_id={chat_id}&message_id={message_id}&text={new_text}"
    )
    if r.ok:
        return r.json()["result"]["message_id"]


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


def get_opts(chat_id, message_id):
    def update_message(text):
        try:
            update_message_blocking(text, chat_id, message_id)
        except Exception as e:
            logger.warning(e, exc_info=True)

    last_status = None

    def status_hook(d):
        nonlocal last_status
        try:
            if d["status"] == "finished" and last_status != "finished":
                update_message(f"Extracting MP3...")
            elif d["status"] == "downloading":
                update_message(
                    f"Downloading...\n{d['_percent_str']} at {d['_speed_str']} ETA: {d['_eta_str']}"
                )
        except Exception as e:
            logger.warning(e, exc_info=True)

    opts = DOWNLOAD_OPTIONS.copy()
    opts["progress_hooks"] = [status_hook]
    return opts


def download_single_url(url, chat_id, message_id):
    opts = get_opts(chat_id, message_id)
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


def download_playlist(url, chat_id, message_id):
    """Download each video in the playlist and return the information as a list of tuples"""
    opts = get_opts(chat_id, message_id)
    with yt_dlp.YoutubeDL({"extract_flat": True}) as flat:
        info = flat.extract_info(url, download=False)
        title = info["title"]
        count = info["playlist_count"]
        send_message_blocking(chat_id, f"{title} ({count} tracks)")

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url)
        for entry in info["entries"]:
            artist, title = get_metadata_local(entry)
            filename = f"/tmp/{entry['id']}.mp3"
            if not os.path.exists(filename):
                raise FileNotFoundError
            set_tags(filename, title, artist)
            yield File(filename, artist, title, url), 0


def download_url(url: str, chat_id, message_id) -> list[File]:
    if "playlist" in url:
        files = []
        for file, exit_code in download_playlist(url, chat_id, message_id):
            if not exit_code:
                files.append(file)
        return files
    else:
        file, exit_code = download_single_url(url, chat_id, message_id)
        if not exit_code:
            return [file]
        raise Exception(f"Could not download from URL: {url}")


if __name__ == "__main__":
    channel_id = -1001213653335
    message_id = send_message_blocking(channel_id, "TEST")
    print(
        download_url(
            "https://music.youtube.com/playlist?list=OLAK5uy_nSimGj4CXHflKeUOh_JjLOnR75Kp6Q064&si=zpprCbpnKTIc1lc6",
            chat_id=channel_id,
            message_id=message_id,
        )
    )
