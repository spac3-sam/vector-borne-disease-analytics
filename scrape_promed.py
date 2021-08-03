import requests
from bs4 import BeautifulSoup as bs
# from requests.api import get
import pandas as pd
import concurrent.futures
import threading

headers = {'user-agent': 'Mozilla/5.0'}

COLUMNS = ['id', 'title', 'zoom_lat', 'zoom_lon', 'zoom_level', 'alert_id', 'feed_id', 'summary', 'issue_date', 'load_date', 'incident_date',
               'descr', 'alert_tag_id', 'dup_count', 'dup_of', 'unique_string', 'info_hash', 'submitted_by', 'reviewed', 'search_string_id', 'content']
df = pd.DataFrame(columns=COLUMNS)

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def get_post_ids(search_term):
    current_page = 0
    search_data = {
        'action': 'get_promed_search_content',
        'query[0][name]': 'pagenum',
        'query[0][value]': '0',
        'query[1][name]': 'kwby1',
        'query[1][value]': 'summary',
        'query[2][name]': 'search',
        'query[2][value]': search_term,
        # 'query[3][name]': 'show_us',
        # 'query[3][value]': '1',
        # 'query[4][name]': 'date1',
        # 'query[4][value]': '',
        # 'query[5][name]': 'date2',
        # 'query[5][value]': '',
        'query[6][name]': 'feed_id',
        'query[6][value]': '1',
        'query[7][name]': 'submit',
        'query[7][value]': 'next',
    }
    r = requests.post('https://promedmail.org/wp-admin/admin-ajax.php',
                      headers=headers, data=search_data).json()
    num_results = r['res_count']
    post_ids = {}
    while len(post_ids) < num_results:
        print(f'Fetching results page {current_page}')
        soup = bs(r['results'], 'html.parser')
        for tag in soup.find_all('a'):
            post_ids[tag['id'][2:]] = tag.contents

        current_page += 1
        search_data['query[0][value]'] = f'{current_page}'
        req = requests.post('https://promedmail.org/wp-admin/admin-ajax.php',
                            headers=headers, data=search_data)
        r = req.json()
    return post_ids

def get_post(args):
    id, title = args
    session = get_session()
    search_data = {
            'action': 'get_latest_post_data',
            'alertId': f'{id}'
        }
    r = session.post('https://promedmail.org/wp-admin/admin-ajax.php',
                        headers=headers, data=search_data).json()
    df.loc[id] = [id, title, r['zoom_lat'], r['zoom_lon'], r['zoom_level'], *
            [r['postinfo'][x] for x in r['postinfo'].keys() if x in COLUMNS]]
    print(f'Finished parsing post #{len(df)}')

def get_posts(search_term):
    post_ids = get_post_ids(search_term)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for _ in executor.map(get_post, post_ids.items()):
            pass # hack so we can sigexit through the main thread

if __name__ == '__main__':
    get_posts('chikungunya')
    df.to_csv('promed_chikungunyaw.csv', sep='\t')


# malaria eda