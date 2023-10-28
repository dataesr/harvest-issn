import urllib
import os
import requests
from retry import retry

from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

OPENALEX_API_KEY = os.getenv('OPENALEX_API_KEY')

@retry(delay=300, tries=5, logger=logger)
def get_volume_from_openalex(issn, year):
    url = f'https://api.openalex.org/works?filter=primary_location.source.issn:{issn},publication_year:{year}&group_by=open_access.oa_status'
    r = requests.get(url).json()
    res = []
    for e in r['group_by']:
        new_e = {'oa_status': e['key'], 'year': year, 'nb_publications': e['count']}
        res.append(new_e)
    return res

@retry(delay=300, tries=5, logger=logger)
def get_publishers_from_openalex(issn):
    url = f'https://api.openalex.org/works?filter=primary_location.source.issn:{issn},from_publication_date:2013-01-01&group_by=primary_location.source.publisher_lineage'
    r = requests.get(url).json()
    res = []
    for e in r['group_by']:
        res.append(e['key_display_name'])
    return res
