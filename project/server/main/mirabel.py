import urllib
import os
import requests
import pandas as pd
from project.server.main.utils import to_jsonl
from retry import retry

from project.server.main.logger import get_logger

logger = get_logger(__name__)

@retry(delay=300, tries=5, logger=logger)
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
            res[f] = titre[f]
    liensext = titre.get('liensext')
    for t in ['ddh', 'doaj', 'openalex', 'scopus', 'wos', 'hal']:
        res[f'infos_{t}'] = {f'is_in_{t}': False} 
    if isinstance(liensext, list):
        for i in liensext:
            if len(i)==2:
                if i[1] == 'DDH':
                    res['infos_ddh']['is_in_ddh'] = True
                    res['infos_ddh']['url'] = i[0]
                if i[1] == 'DOAJ':
                    res['infos_doaj']['is_in_doah'] = True
                    res['infos_doaj']['url'] = i[0]
                if i[1] == 'HAL':
                    res['infos_hal']['is_in_hal'] = True
                    res['infos_hal']['url'] = i[0]
                if i[1] == 'OpenAlex':
                    res['infos_openalex']['is_in_openalex'] = True
                    res['infos_openalex']['url'] = i[0]
                if i[1] == 'WOS':
                    res['infos_wos']['is_in_wos'] = True
                    res['infos_wos']['url'] = i[0]
                if i[1] == 'SCOPUS':
                    res['infos_scopus']['is_in_scopus'] = True
                    res['infos_scopus']['url'] = i[0]
    return res
