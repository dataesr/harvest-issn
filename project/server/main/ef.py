import requests
import pandas as pd
import os

from project.server.main.logger import get_logger

logger = get_logger(__name__)

ENTITY_FISHING_SERVICE = os.getenv('ENTITY_FISHING_SERVICE')

def get_ef(mytext):
    text = mytext
    params = {
        #"text": text,
        "shortText": text,
        "language": {"lang": "fr"},
        "mentions": [ "wikipedia", "ner"],
    }


    r = requests.post(f"{ENTITY_FISHING_SERVICE}/service/disambiguate", json = params)
    res = r.json()
    return res

def compute_ef(x):
    for ix, e in enumerate(x):
        if ix % 25 == 0:
            logger.debug(f'{ix} / {len(x)}')
        if isinstance(e.get('text'), str):
            try:
                res = get_ef(e['text'])
                e['ef'] = res
            except:
                logger.debug(f"error with {e['text']}")
                pass
    return x
