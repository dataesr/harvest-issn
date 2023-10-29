import os
import sys
import pandas as pd
from urllib import parse

from project.server.main.harvest import harvest
from project.server.main.parse import parse_issn
from project.server.main.utils import to_jsonl
from project.server.main.openalex import get_volume_from_openalex, get_publishers_from_openalex
from project.server.main.utils_swift import upload_object, download_object, exists_in_storage
from project.server.main.elastic import reset_index

from project.server.main.logger import get_logger

logger = get_logger(__name__)

ES_LOGIN_BSO_BACK = os.getenv('ES_LOGIN_BSO_BACK', '')
ES_PASSWORD_BSO_BACK = os.getenv('ES_PASSWORD_BSO_BACK', '')
ES_URL = os.getenv('ES_URL', 'http://localhost:9200')

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def is_valid_issn(x):
    if not isinstance(x, str):
        return False
    if len(x) != 9:
        return False
    if x[4:5] != '-':
        return False
    if '--' in x:
        return False
    for e in ['.', '*']:
        assert(e not in x)
    return True

def create_task_harvest(args: dict) -> None:
    harvest_date = args.get('harvest_date')
    ix = args.get('ix', 0)
    if harvest_date is None:
        logger.debug('missing harvest_date argument')
        return
    issns = args.get('issns', [])
    logger.debug(f'{len(issns)} issns to harvest and parse')
    all_parsed = []
    for issn in issns:
        source = None
        if args.get('download', True):
            source = harvest(harvest_date, issn)
        if args.get('parse', True):
            if source is None:
                try:
                    download_object('issn', f'{harvest_date}/raw/{issn}.html', f'{issn}.html')
                    current_file = open(f'{issn}.html', 'r')
                    source = current_file.read()
                    current_file.close()
                    os.system(f'rm -rf {issn}.html')
                except:
                    continue
            parsed_issn = parse_issn(source, issn)
            all_parsed.append(parsed_issn)
    pd.DataFrame(all_parsed).to_json(f'parsed_issn_{ix}.jsonl', lines=True, orient='records')
    upload_object('issn', f'parsed_issn_{ix}.jsonl', f'{harvest_date}/parsed/parsed_issn_{ix}.jsonl')

def create_task_collect(args):
    harvest_date = args.get('harvest_date')
    fr_country = set(['France', 'Guadeloupe', 'Martinique'])
    all_df = []
    for ix in range(0, 1000):
        current_file = f'{harvest_date}/parsed/parsed_issn_{ix}.jsonl'
        if exists_in_storage( 'issn', current_file):
            download_object('issn', current_file, f'parsed_issn_{ix}.jsonl')
            df_tmp = pd.read_json(f'parsed_issn_{ix}.jsonl', lines=True, orient='records')
            df_tmp_filtered = df_tmp[df_tmp.country.apply(lambda x: isinstance(x, list) and x[0] in fr_country)]
            logger.debug(f'{len(df_tmp_filtered)} french ISSN in {current_file}')
            all_df.append(df_tmp_filtered)
        else:
            break
    df = pd.concat(all_df)
    df.to_json('french_issns.jsonl', lines=True, orient='records')
    logger.debug(f'{len(df)} french ISSN identified')
    upload_object('issn', f'french_issns.jsonl', f'{harvest_date}/french_issns.jsonl')

def create_task_enrich(args):
    harvest_date = args.get('harvest_date')
    if args.get('enrich', False):
        download_object('issn', f'{harvest_date}/french_issns.jsonl', f'french_issns.jsonl')
        data = pd.read_json(f'french_issns.jsonl', lines=True, orient='records').to_dict(orient='records')
        data_enriched = []
        nb_issn = len(data)
        for ix, d in enumerate(data):
            issn = d.get('issn_l')[0]
            assert(is_valid_issn(issn))
            publications_info = {'counts': []}
            logger.debug(f'openalex enrichment for {issn}, {ix}/{nb_issn}')
            for year in range(2013, 2025):
                publications_info['counts'] += get_volume_from_openalex(issn, year)
            publications_info['publishers'] = get_publishers_from_openalex(issn)
            d['publications_info'] = publications_info
            data_enriched.append(d)
        to_jsonl(data_enriched, f'french_issns_enriched.jsonl')
        upload_object('issn', f'french_issns_enriched.jsonl', f'{harvest_date}/french_issns_enriched.jsonl')
    if args.get('load', False):
        download_object('issn', f'{harvest_date}/french_issns_enriched.jsonl', f'french_issns_enriched.jsonl')
        data = pd.read_json(f'french_issns_enriched.jsonl', lines=True, orient='records').to_dict(orient='records')
        new_data = []
        for d in data:
            for f in ['title', 'country', 'medium', 'last_modification_date', 'record_type', 'responsible_issn_center', 
                    'issn_l', 'title_abbreviated', 'description', 'frequency', 'resource_type', 'language', 'publisher']:
                if isinstance(d.get(f), list):
                    d[f] = d[f][0]
            new_data.append(d)
        to_jsonl(new_data, f'french_issns_enriched_to_load.jsonl')
        import_es(f'bso-issn-{harvest_date}')

def import_es(index_name):
    input_file = f'french_issns_enriched_to_load.jsonl'
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading bso-issn index')
    reset_index(index=index_name)
    elasticimport = f"elasticdump --input={input_file} --output={es_host}{index_name} --type=data --limit 1000 " + "--transform='doc._source=Object.assign({},doc)'"
    logger.debug('starting import in elastic')
    os.system(elasticimport)
