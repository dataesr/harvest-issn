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

#theme
#https://reseau-mirabel.info/api/themes/grappe/89

@retry(delay=100, tries=5, logger=logger)
def get_mirabel_dump():
    df = pd.read_json(f'https://reseau-mirabel.info/grappe/export?dest=bso&api-key={MIRABEL_API_KEY}', compression='gzip')
    logger.debug(f"{len(df.titres)} titres récupérés dans Mirabel")
    mirabel_data = df.titres.to_list()
    revue_map = {}
    for d in mirabel_data:
        if d['revueid'] not in revue_map:
            revue_map[d['revueid']] = []
        revue_map[d['revueid']].append(d)
    revues_data = []
    for r in revue_map:
        try:
            revue_map[r].sort(key=lambda x:x['dates']['debut'], reverse=True) # plus récente en premier
        except:
            revue_map[r].sort(key=lambda x:x['titreid'], reverse=True) # plus recent en premier
        current_revue = {}
        # à partir de la plus recente
        for f in ['revueid', 'titre', 'sigle', 'siteweb', 'periodicite', 'langues']:
            current_revue[f] = revue_map[r][0].get(f)
        current_revue['issns'] = []
        current_revue['titres'] = []
        liens = {}
        wikipedia = {}
        min_date = 9999
        for revue in revue_map[r]:
            if revue.get('dates') and revue['dates'].get('debut'):
                min_date = min(min_date, int(revue['dates'].get('debut')[0:4]))
            current_revue['titres'].append(revue['titreid'])
            ident = revue['identifiants']
            for k in ['issne', 'issnp', 'issnl']:
                if ident.get(k) and ident.get(k) not in current_revue['issns']:
                    current_revue['issns'].append(ident.get(k))
            for k in revue.get('liens'):
                if revue['liens'][k]:
                    liens[k] = True
            for k in revue.get('wikipedia'):
                if k:
                    wikipedia[k] = True
        current_revue['dates'] = {'debut': None}
        if min_date != 9999:
            current_revue['dates'] = {'debut': str(min_date)}
        current_revue['dates']['fin'] = revue_map[r][0]['dates']['fin']
        current_revue['liens'] = liens
        current_revue['wikipedia'] = {}
        for k in wikipedia:
            lang = k.replace('https://', '').split('.')[0]
            language = get_lang(lang)
            current_revue['wikipedia'][language] = True
        revues_data.append(current_revue)
    os.system('rm -rf /upw_data/mirabel/mirabel.jsonl')
    to_jsonl(revues_data, '/upw_data/mirabel/mirabel.jsonl')
    return {'update': df.miseajour.max(), 'data': revues_data}

def get_issns(d):
    return d.get('issns', [])
    #issns = []
    #if isinstance(d.get('identifiants'), dict):
    #    for f in ['issne', 'issnp', 'issnl']:
    #        if d['identifiants'].get(f):
    #            assert(isinstance(d['identifiants'][f], str))
    #            issns.append(d['identifiants'][f])
    #return list(set(issns))

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

def get_lang(code: str) -> str:
    """
    Convertit un code langue Wikipédia ('fr', 'en', 'de', ...)
    en nom de langue en français.
    """
    languages = {
        "fr": "français",
        "en": "anglais",
        "de": "allemand",
        "es": "espagnol",
        "it": "italien",
        "pt": "portugais",
        "nl": "néerlandais",
        "ru": "russe",
        "zh": "chinois",
        "ja": "japonais",
        "ko": "coréen",
        "ar": "arabe",
        "pl": "polonais",
        "sv": "suédois",
        "uk": "ukrainien",
        "cs": "tchèque",
        "tr": "turc",
        "he": "hébreu",
        "fi": "finnois",
        "no": "norvégien",
        "da": "danois",
        "el": "grec",
        "hu": "hongrois",
        "ro": "roumain",
        "ca": "catalan",
        "fa": "persan",
    "oc": "occitan",
    "az": "azéri",
    "gl": "galicien",
    "la": "latin",
    "bg": "bulgare",
    "br": "breton",
    "et": "estonien",
    "is": "islandais",
    "ms": "malais",
    "sl": "slovène",
    "ta": "tamoul",
    "uz": "ouzbek",
    "eu": "basque",
    "eo": "espéranto",
    "id": "indonésien",
    "ig": "igbo",
    "ha": "haoussa",
    }
    return languages.get(code.lower(), f"langue inconnue ({code})")
