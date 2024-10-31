import io
import logging
import os
import wave
from typing import NamedTuple, Type

import boto3
import requests
import yt_dlp
from mutagen.easyid3 import EasyID3
from yt_dlp.cache import Cache

from yt_downloader_cache import S3PersistentCache

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

    # Move the buffer position to the beginning
    buffer.seek(0)
    return buffer


def send_dummy_audio_message(chat_id) -> int:
    audio = create_dummy_audio()
    r = requests.get(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendAudio?chat_id={chat_id}",
        files={
            "audio": audio.read(),
            "title": "placeholder audio...",
            "caption": "This is a placeholder...",
        },
    )
    if r.ok:
        print(r.json()["result"]["message_id"])
        return r.json()["result"]["message_id"]
    else:
        print("FAILED", r.status_code, r.text)


def delete_message_blocking(chat_id, message_id):
    requests.get(
        f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage?chat_id={chat_id}&message_id={message_id}"
    )


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


s3_client = boto3.client("s3")


class Downloader(yt_dlp.YoutubeDL):
    def __init__(self, options, cache_cls: Type[Cache] = S3PersistentCache):
        options["username"] = "oauth2"
        options["password"] = ""
        super().__init__(options)
        self.cache = cache_cls(self)


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


def update_message(text, chat_id, message_id):
    if not chat_id:
        return
    try:
        update_message_blocking(text, chat_id, message_id)
    except Exception as e:
        logger.warning(e, exc_info=True)


downloading = False
finished = False


def status_hook(chat_id, message_id, d):
    global downloading, finished
    if not chat_id:
        return
    try:
        if d["status"] == "finished" and not finished:
            finished = True
            downloading = False
            update_message("Extracting MP3...", chat_id, message_id)
        elif d["status"] == "downloading" and not downloading:
            downloading = True
            finished = False
            update_message(
                f"Downloading...",
                chat_id,
                message_id,
            )
    except Exception as e:
        logger.warning(e, exc_info=True)


def get_opts(chat_id=None, message_id=None):
    opts = DOWNLOAD_OPTIONS.copy()
    opts["progress_hooks"] = [lambda d: status_hook(chat_id, message_id, d)]
    return opts


def download_single_url(url, chat_id=None, message_id=None, cache_cls=Cache):
    opts = get_opts(chat_id, message_id)
    with Downloader(opts, cache_cls) as ydl:
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


def download_playlist(url, chat_id=None, message_id=None, cache_cls=Cache):
    """Download each video in the playlist and return the information as a list of tuples"""
    with Downloader({"extract_flat": True}, cache_cls) as flat:
        info = flat.extract_info(url, download=False)
        title = info["title"]
        count = info["playlist_count"]
        send_message_blocking(chat_id, f"{title} ({count} tracks)")
    for entry in info["entries"]:
        result, exit_code = download_single_url(
            entry["url"], chat_id, message_id, cache_cls
        )
        if not exit_code:
            yield result


def download_url(url: str, chat_id=None, message_id=None, cache_cls=Cache):
    if "playlist" in url:
        return download_playlist(url, chat_id, message_id)
    else:
        file, exit_code = download_single_url(url, chat_id, message_id, cache_cls)
        if not exit_code:
            return (f for f in [file])
        raise Exception(f"Could not download from URL: {url}")


if __name__ == "__main__":
    print(download_url("https://www.youtube.com/watch?v=bgWUwywrXOM"))
