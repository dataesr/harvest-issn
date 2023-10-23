import sys
import pandas as pd

from project.server.main.harvest import harvest
from project.server.main.parse import parse
from project.server.main.utils_swift import upload_object, download_object

from project.server.main.logger import get_logger

logger = get_logger(__name__)


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
    if harvest_date is None:
        logger.debug('missing harvest_date argument')
        return
    issns = args.get('issns', [])
    if len(issns) == 0:
        df = pd.read_json('/upw_data/issn_l', lines=True)
        issns = [e['journal_issn_l'] for e in df.dropna().to_dict(orient='records') if is_valid_issn(e['journal_issn_l'])]
    logger.debug(f'{len(issns)} issns to harvest and parse')
    all_parsed = []
    for issn in issns:
        source = None
        if args.get('download', True):
            source = harvest(harvest_date, issn)
        if args.get('parse', True):
            if source is None:
                download_object('issn', f'{harvest_date}/raw/{issn}.html', f'{issn}.html')
                current_file = open(f'{issn}.html', 'r')
                source = current_file.read()
                current_file.close()
                os.system(f'rm -rf {issn}.html')
            parsed_issn = parse(source, issn)
            all_parsed.append(parsed_issn)
    pd.DataFrame(all_parsed).to_json('parsed_issn.jsonl', lines=True, orient='records')
    upload_object('issn', 'parsed_issn.jsonl', f'{harvest_date}/parsed_issn.jsonl')

