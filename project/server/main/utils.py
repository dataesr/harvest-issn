import os
import re
import json
import string

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def clean_json(elt):
    if isinstance(elt, dict):
        keys = list(elt.keys()).copy()
        for f in keys:
            if (not elt[f] == elt[f]) or (elt[f] is None):
                del elt[f]
            else:
                elt[f] = clean_json(elt[f])
    elif isinstance(elt, list):
        for ix, k in enumerate(elt):
            elt[ix] = clean_json(elt[ix])
    return elt

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            new = clean_json(entry)
            json.dump(new, outfile)
            outfile.write('\n')

