import urllib
import os
import re
import requests
import shutil
import datetime
from project.server.main.logger import get_logger

logger = get_logger(__name__)

UPW_API_KEY = os.getenv('UPW_API_KEY')
url_snapshot = f'http://api.unpaywall.org/feed/snapshot?api_key={UPW_API_KEY}'
MOUNTED_VOLUME = '/upw_data/'

def get_filename_from_cd(cd: str):
    """ Get filename from content-disposition """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]

def download_file(url: str, destination: str = None) -> str:
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    start = datetime.datetime.now()
    with requests.get(url, stream=True, verify=False) as r:
        r.raise_for_status()
        try:
            local_filename = get_filename_from_cd(r.headers.get('content-disposition')).replace('"', '')
        except:
            local_filename = url.split('/')[-1]
        logger.debug(f'Start downloading {local_filename} at {start}')
        local_filename = f'{MOUNTED_VOLUME}{local_filename}'
        if destination:
            local_filename = destination
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'End download in {delta}')
    return local_filename

def download_snapshot(arg):
    if arg.get('download_snapshot', False):
        snapshot_file = download_file(url_snapshot, '/upw_data/unpaywall_snapshot.gz')
    cmd = f"zcat /upw_data/unpaywall_snapshot.gz"
    cmd += " | jq -c {journal_issn_l} | grep -v null | sort -u > /upw_data/issn_l.jsonl"
    logger.debug(cmd)
    os.system(cmd)


