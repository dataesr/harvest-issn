import os

from elasticsearch import Elasticsearch, helpers

from project.server.main.decorator import exception_handler
from project.server.main.logger import get_logger

ES_LOGIN_BSO_BACK = os.getenv('ES_LOGIN_BSO_BACK', '')
ES_PASSWORD_BSO_BACK = os.getenv('ES_PASSWORD_BSO_BACK', '')
ES_URL = os.getenv('ES_URL', 'http://localhost:9200')

client = None
logger = get_logger(__name__)


@exception_handler
def get_client():
    global client
    if client is None:
        client = Elasticsearch(ES_URL, http_auth=(ES_LOGIN_BSO_BACK, ES_PASSWORD_BSO_BACK))
    return client

@exception_handler
def delete_index(index: str) -> None:
    logger.debug(f'Deleting {index}')
    es = get_client()
    response = es.indices.delete(index=index, ignore=[400, 404])
    logger.debug(response)


@exception_handler
def update_alias(alias: str, old_index: str, new_index: str) -> None:
    es = get_client()
    logger.debug(f'updating alias {alias} from {old_index} to {new_index}')
    response = es.indices.update_aliases({
        'actions': [
            {'remove': {'index': old_index, 'alias': alias}},
            {'add': {'index': new_index, 'alias': alias}}
        ]
    })
    logger.debug(response)

def get_analyzers() -> dict:
    return {
        'light': {
            'tokenizer': 'icu_tokenizer',
            'filter': [
                'lowercase',
                'french_elision',
                'icu_folding'
            ]
        }
    }

def get_filters() -> dict:
    return {
        'french_elision': {
            'type': 'elision',
            'articles_case': True,
            'articles': ['l', 'm', 't', 'qu', 'n', 's', 'j', 'd', 'c', 'jusqu', 'quoiqu', 'lorsqu', 'puisqu']
        }
    }

@exception_handler
def reset_index(index: str) -> None:
    es = get_client()
    delete_index(index)
    
    settings = {
        'analysis': {
            'filter': get_filters(),
            'analyzer': get_analyzers()
        }
    }
    
    dynamic_match = None

    mappings = { 'properties': {} }
    response = es.indices.create(
        index=index,
        body={'settings': settings, 'mappings': mappings},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response and response['acknowledged']:
        response = str(response['index'])
        logger.debug(f'Index mapping success for index: {response}')


@exception_handler
def load_in_es(data: list, index: str) -> list:
    es = get_client()
    actions = [{'_index': index, '_source': datum} for datum in data]
    ix = 0
    indexed = []
    for success, info in helpers.parallel_bulk(client=es, actions=actions, chunk_size=500, request_timeout=60,
                                               raise_on_error=False):
        if not success:
            logger.debug(f'A document failed: {info}')
        else:
            indexed.append(data[ix])
        ix += 1
    logger.debug(f'{len(data)} elements imported into {index}')
    return indexed
