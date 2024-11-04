import os

S3_BUCKET = os.environ.get("S3_BUCKET", "dlbot")
CACHE_KEY = "/cache"
MAX_AUDIO_UPDATE_RETRIES = 5
