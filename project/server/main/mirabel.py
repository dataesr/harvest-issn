import urllib
import os
import requests
import pandas as pd
from project.server.main.utils import to_jsonl
from project.server.main.mongo import get_doi_from_issn
from project.server.main.utils import chunks
from project.server.main.utils_swift import upload_object, download_object
from retry import retry

from project.server.main.logger import get_logger

logger = get_logger(__name__)

MIRABEL_API_KEY = os.getenv('MIRABEL_API_KEY')

@retry(delay=100, tries=5, logger=logger)
def get_mirabel_dump():
    df = pd.read_json(f'https://reseau-mirabel.info/grappe/export?dest=bso&api-key={MIRABEL_API_KEY}', compression='gzip')
    logger.debug(f"{len(df.titres)} titres récupérés dans Mirabel")
    mirabel_data = df.titres.to_list()
    to_jsonl(mirabel_data, '/upw_data/mirabel/mirabel.jsonl')
    return {'update': df.miseajour.max(), 'data': df.titres.to_list()}

def get_issns(d):
    issns = []
    if isinstance(d.get('identifiants'), dict):
        for f in ['issne', 'issnp', 'issnl']:
            if d['identifiants'].get(f):
                assert(isinstance(d['identifiants'][f], str))
                issns.append(d['identifiants'][f])
    return list(set(issns))

def get_all_issns(mirabel_dump):
    issns = []
    for d in mirabel_dump['data']:
        issns += get_issns(d)
    issns = list(set(issns))
    logger.debug(f"{len(issns)} issns found")
    return issns

def get_bso_local_mirabel(mirabel_dump):
    all_issns = get_all_issns(mirabel_dump)
    issn_chunks = chunks(all_issns, 50)
    all_dois = []
    for issn_chunk in issn_chunks:
        dois = [k['doi'] for k in get_doi_from_issn(issn_chunk) if k.get('doi')]
        logger.debug(f"got {len(dois)} DOIs for a chunk of {len(issn_chunk)} ISSNs")
        all_dois += list(set(dois))
    all_dois = list(set(all_dois))
    logger.debug(f"total {len(all_dois)} retrievied for the {len(all_issns)} ISSNs")
    bso_local_data = [{'doi': doi, 'bso_country': 'other'} for doi in all_dois]
    pd.DataFrame(bso_local_data).to_csv('bsoedition.csv', index=False)
    # uploading to bso_local bucket
    upload_object('bso-local', f'bsoedition.csv', f'bsoedition.csv')

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
