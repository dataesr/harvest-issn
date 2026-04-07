import urllib
import os
import requests
import pandas as pd
from project.server.main.utils import to_jsonl
from retry import retry

from project.server.main.logger import get_logger

logger = get_logger(__name__)

@retry(delay=100, tries=5, logger=logger)
def get_mirabel_infos(revue_id):
    url = f"https://reseau-mirabel.info/api/revues/{revue_id}"
    res = requests.get(url).json()
    return res

def get_mirabel_for_ids(ids):
    mirabel_data = []
    for rid in ids:
        logger.debug(f'get {rid} from mirabel')
        mirabel_data.append(get_mirabel_infos(rid))
    os.system(f'mkdir -p /upw_data/mirabel')
    pd.DataFrame(mirabel_data).to_json('/upw_data/mirabel/raw.jsonl', orient='records', lines=True)
    logger.debug(f"{len(mirabel_data)} lines from mirabel written in /upw_data/mirabel/raw.jsonl")
    return mirabel_data

def parse_all_mirabel():
    data = pd.read_json('/upw_data/mirabel/raw.jsonl', orient='records', lines=True).to_dict(orient='records')
    parsed = []
    for d in data:
        p = parse_mirabel(d)
        parsed.append(p)
    os.system(f'rm -rf /upw_data/mirabel/parsed.jsonl')
    to_jsonl(parsed, "/upw_data/mirabel/parsed.jsonl")
    logger.debug(f"{len(parsed)} parsed lines from mirabel written in /upw_data/mirabel/parsed.jsonl")

def parse_mirabel(notice):
    res = {}
    res['revue_id'] = notice['id']
    titres = notice['titres']
    res['nb_titres'] = len(titres)
    titre = titres[0]
    for f in ['url', 'periodicite', 'langues', 'editeurs', 'titre', 'sigle', 'datedebut', 'datefin', 'issns', 'labellisation']:
        if titre.get(f):
            if f in ['datedebut', 'datefin']:
                if isinstance(titre[f], str) and len(titre[f])>=4:
                    res[f] = titre[f][0:4]
            else:
                res[f] = titre[f]
    liensext = titre.get('liensext')
    platforms = ['ddh', 'doaj', 'openalex', 'scopus', 'wos', 'hal']
    for p in platforms:
        res[f'infos_{p}'] = {f'is_in_{p}': False} 
    if isinstance(liensext, list):
        for i in liensext:
            if len(i)==2:
                platform = i[1].lower().replace(' ', '_').strip()
                for p in platforms:
                    if platform == p:
                        res[f'infos_{p}'][f'is_in_{p}'] = True
                        res[f'infos_{p}']['url'] = i[0]
    return res
