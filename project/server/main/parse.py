import urllib
import os
from bs4 import BeautifulSoup

from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)


field_names = {
    'Title proper:': 'title',
    'Other variant title': 'title_variant',
    'Abbreviated key-title:': 'title_abbreviated',
    'Parallel title:': 'title_parallel',
    'Original alphabet of title': 'title_original_alphabel',
    'Subject:': 'subject',
    'Publisher:': 'publisher',
    'Description:': 'description',
    'Frequency:': 'frequency',
    'Type of resource:': 'resource_type',
    'Language:': 'language',
    'Country:': 'country',
    'Medium:': 'medium',
    'Last modification date:': 'last_modification_date',
    'Type of record:': 'record_type',
    'ISSN Center responsible of the record:': 'responsible_issn_center'
}

def parse_issn(source, input_issn):
    soup = BeautifulSoup(source, 'lxml')
    res = {'input_issn': input_issn, 'the_keepers': False, 'road': False, 'in_mirabel': False}
    for item_result in soup.find_all(class_='item-result-content-text'):
        for sp in item_result.find_all('p'):
            if sp.find('span'):
                for f in field_names:
                    if sp.find('span').get_text().strip() == f:
                        current_field = field_names[f]
                        if current_field not in res:
                            res[current_field] = []
                        current_value = sp.get_text().replace(f, '').strip()
                        if current_value not in res[current_field]:
                            res[current_field].append(current_value)
                if sp.find('span').get_text().strip() == 'Indexed by:':
                    if 'THE KEEPERS' in sp.get_text():
                        res['the_keepers'] = True
                    elif 'ROAD' in sp.get_text():
                        res['road'] = True
                    elif sp.find('a') and 'wikidata' in sp.find('a').attrs['href']:
                        res['wikidata'] = sp.find('a').attrs['href'].split('/')[-1]
                    #print(sp)
    for k in ['issn', 'issn_l', 'incorrect_issn', 'url']:
        res[k] = []
    for e in soup.find_all(class_='sidebar-accordion-list-selected-item'):
        a_elt = e.find('a')
        if a_elt:
            skip=False
            for k in ['google', 'bing', 'yahoo', 'pubmed', '.gov']:
                if k in a_elt.attrs['href']:
                    skip=True
            if skip:
                continue
            #print(a_elt)
            current_value = a_elt.get_text().strip()
            if 'ISSN-L' in e.get_text():
                if current_value not in res['issn_l']  and '-' in current_value and len(current_value)==9:
                    res['issn_l'].append(current_value)
            elif 'Incorrect' in e.get_text():
                if current_value not in res['incorrect_issn']:
                    res['incorrect_issn'].append(current_value)
            elif 'ISSN' in e.get_text():
                if current_value not in res['issn'] and '-' in current_value and len(current_value)==9:
                    res['issn'].append(current_value)
            elif 'URL' in e.get_text():
                res['url'] = a_elt.attrs['href']

            if 'reseau-mirabel' in a_elt.attrs['href']:
                res['in_mirabel'] = True
                res['mirabel_url'] = a_elt.attrs['href']
    return res
