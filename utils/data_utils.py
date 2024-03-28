import logging
import hashlib
from typing import Any
import pandas as pd
import yaml
import json
from .log_utils import logger


GCP_PROJECT_ID = "esg-rag"

def read_data(path: str) -> Any:
    if path.endswith(".json"):
        if path.startswith("gs://"):
            import gcsfs

            fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
            with fs.open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
    elif path.endswith(".txt"):
        with open(path, "r", encoding="utf-8") as f:
            data = f.read()
        data = data.split("\n")
    elif path.endswith(".csv"):
        data = pd.read_csv(path)
    elif path.endswith(".ndjson"):
        data = pd.read_json(path, lines=True, orient="records")
    elif path.endswith(".ndjson.gz"):
        data = pd.read_json(path, lines=True, orient="records", compression="gzip")
    elif path.endswith(".pickle"):
        data = pd.read_pickle(path)
    elif path.endswith(".parquet"):
        data = pd.read_parquet(path)
    elif path.endswith(".yaml"):
        with open(path, "r") as stream:
            try:
                data = yaml.safe_load(stream)
            except yaml.YAMLError as e:
                logging.error(e)
    else:
        data = []
    return data


def save_data(data: Any, path: str) -> None:
    if path.endswith(".json"):
        if path.startswith("gs://"):
            import gcsfs
            
            fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
            with fs.open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        else:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
    elif path.endswith(".txt") and isinstance(data, list):
        with open(path, "w", encoding="utf-8") as f:
            for _d in data:
                f.write(_d)
                f.write("\n")
    elif path.endswith(".csv"):
        data.to_csv(path, index=False) # data.to_csv(path, mode='a', header=False, index=False)
    elif path.endswith(".ndjson"):
        data.to_json(path, lines=True, orient="records")
    elif path.endswith(".ndjson.gz"):
        data.to_json(path, lines=True, orient="records", compression="gzip")
    elif path.endswith(".pickle"):
        data.to_pickle(path)
    elif path.endswith(".parquet"):
        data.to_parquet(path)
    elif isinstance(data, list):
        with open(path, "w", encoding="utf-8") as f:
            for _d in data:
                f.write(_d)
                f.write("\n")
    else:
        pass


def string_slugify(name: str, allow_unicode: bool = False) -> bool:
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """

    import unicodedata
    import re

    name = str(name)
    if allow_unicode:
        name = unicodedata.normalize("NFKC", name)
    else:
        name = (
            unicodedata.normalize("NFKD", name)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    name = re.sub(r"[^\w\s-]", "", name.lower())
    return re.sub(r"[-\s]+", "-", name).strip("-_")


def remove_non_chinese_charaters(string):
    import re

    filtrate = re.compile("[^\u4E00-\u9FA5]")  # non-Chinese unicode range
    string = filtrate.sub(r"", string)  # remove all non-Chinese characters
    return string


def hash_string_to_ID(string: str):
    m = hashlib.md5()
    try:
        m.update(string.encode("utf-8"))
    except Exception as err:
        logger.error(err)
        m.update(string)
    return int(str(int(m.hexdigest(), 16))[0:12])

