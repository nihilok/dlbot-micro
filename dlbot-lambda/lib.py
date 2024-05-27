import asyncio
import logging
import os
from typing import NamedTuple

import yt_dlp
from mutagen.easyid3 import EasyID3
from yt_dlp.postprocessor import FFmpegPostProcessor

# FFmpegPostProcessor._ffmpeg_location.set("/usr/local/bin")

logger = logging.getLogger(__name__)

DOWNLOAD_OPTIONS = {
    "paths": {"home": "/tmp/", "temp": "/tmp/"},
    "outtmpl": "/tmp/%(id)s.%(ext)s",
    "format": "bestaudio/best",
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


def download_single_url(url):
    with yt_dlp.YoutubeDL(DOWNLOAD_OPTIONS) as ydl:
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


def download_playlist(url, send_message=None):
    """Download each video in the playlist and return the information as a list of tuples"""
    with yt_dlp.YoutubeDL({"extract_flat": True}) as ydl:
        info = ydl.extract_info(url)
        for entry in info["entries"]:
            yield download_single_url(entry["url"])


def download_url(url: str) -> list[File]:
    if "playlist" in url:
        files = []
        for file, exit_code in download_playlist(url):
            if not exit_code:
                files.append(file)
            continue
        return files
    else:
        file, exit_code = download_single_url(url)
        if not exit_code:
            return [file]
        raise Exception(f"Could not download from URL: {url}")


if __name__ == "__main__":
    import sys

    download_single_url(sys.argv[1])
