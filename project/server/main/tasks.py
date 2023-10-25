import sys
import pandas as pd

from project.server.main.harvest import harvest
from project.server.main.parse import parse
from project.server.main.utils_swift import upload_object, download_object

from project.server.main.logger import get_logger

logger = get_logger(__name__)

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
            parsed_issn = parse(source, issn)
            all_parsed.append(parsed_issn)
    pd.DataFrame(all_parsed).to_json('parsed_issn_{ix}.jsonl', lines=True, orient='records')
    upload_object('issn', 'parsed_issn_{ix}.jsonl', f'{harvest_date}/parsed/parsed_issn_{ix}.jsonl')

