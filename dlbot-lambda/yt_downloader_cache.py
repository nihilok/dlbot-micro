import json
import logging
import traceback

from yt_dlp.cache import Cache

from boto3_clients import s3_client
from constants import CACHE_KEY, S3_BUCKET


class S3PersistentCache(Cache):
    S3_BUCKET = S3_BUCKET
    CACHE_KEY = CACHE_KEY

    def store(self, section, key, data, dtype="json"):
        from yt_dlp.cache import __version__

        try:
            self._ydl.write_debug(f"Saving {section}.{key} to cache")
            string = json.dumps({"yt-dlp_version": __version__, "data": data})
            s3_client.put_object(
                Bucket=self.S3_BUCKET,
                Key=f"{self.CACHE_KEY}/{section}/{key}",
                Body=string,
            )
        except Exception:
            tb = traceback.format_exc()
            self._ydl.report_warning(
                f"Writing cache to {self.S3_BUCKET}{self.CACHE_KEY} failed: {tb}"
            )

    def load(self, section, key, dtype="json", default=None, *, min_ver=None):
        logger = logging.getLogger(__name__)
        self._ydl.write_debug(f"Loading {section}.{key} from cache")
        s3_key = f"{self.CACHE_KEY}/{section}/{key}"
        try:
            obj = s3_client.get_object(Bucket=self.S3_BUCKET, Key=s3_key)
            logger.info(type(obj["Body"]))
            return self._validate(json.loads(obj["Body"].read()), min_ver)
        except Exception as e:
            self._ydl.report_warning(f"Cache retrieval from {s3_key} failed)")
            logger.warning(e, exc_info=True)

        return default
