import urllib
import os

from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

USERNAME = os.getenv('BRIGHT_USERNAME')
PASSWORD = os.getenv('BRIGHT_PASSWORD')
PORT = os.getenv('BRIGHT_PORT')

def get_issn_html(issn):
    url = 'https://portal.issn.org/resource/ISSN/'+issn
    opener = urllib.request.build_opener(
    urllib.request.ProxyHandler(
            {'http': f'{USERNAME}:{PASSWORD}@zproxy.lum-superproxy.io:{PORT}',
            'https': f'{USERNAME}:{PASSWORD}@zproxy.lum-superproxy.io:{PORT}'}))
    #print(f'BRIGHT url = {url}', flush=True)
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        source = urllib.request.urlopen(req).read()
    except:
        logger.debug(f'crawl error for {url}')
        return None
    #source = opener.open(url).read()
    if type(source) == bytes:
        try:
            source = source.decode("utf-8")
        except:
            source = str(source)
    return source

def harvest(harvest_date, issn):
    source = get_issn_html(issn)
    if source is None:
        return None
    current_file = open(f'{issn}.html', 'w')
    current_file.write(source)
    current_file.close()
    upload_object('issn', f'{issn}.html', f'{harvest_date}/raw/{issn}.html')
    os.system(f'rm -rf {issn}.html')
    return source
