import urllib
import os
from retry import retry

from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

OPENALEX_API_KEY = os.getenv('OPENALEX_API_KEY')

@retry(delay=300, tries=5, logger=logger)
def get_volume_from_openalex(issn, year):
    url = f'https://api.openalex.org/works?filter=primary_location.source.issn:{issn},publication_year:{year}&group_by=open_access.oa_status&api_key={OPENALEX_API_KEY}'
    r = requests.get(url).json()
    res = {}
    res = r['group_by']
    return res

