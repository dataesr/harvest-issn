import os
import pymongo
from retry import retry
from project.server.main.logger import get_logger

logger = get_logger(__name__)
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://mongo:27017/')
client = None
def get_client():
    global client
    if client is None:
        client = pymongo.MongoClient(MONGO_URL, connectTimeoutMS=60000)
    return client


def get_database(database: str = 'unpaywall'):
    _client = get_client()
    db = _client[database]
    return db


@retry(delay=200, tries=2)
def get_collection(collection_name: str, database='unpaywall'):
    db = get_database(database)
    collection = db[collection_name]
    return collection

@retry(delay=60, tries=5)
def get_doi_from_issn(issns) -> dict:
    collection = get_collection(collection_name='global')
    res = {}
    res = list(collection.find(
        {'journal_issn_l': {'$in': issns}, 'year': {'$gte': 2013}},
        {'_id': 0, 'doi': 1}
    ))
    return res
