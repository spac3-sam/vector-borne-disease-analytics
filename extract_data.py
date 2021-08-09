import os
os.environ['SPACY_MODEL_SHORTCUT_LINK'] = 'en_core_web_trf'

import spacy
spacy.require_gpu()

import sys
sys.path.append('../EpiTator')

from epitator.annotator import AnnoDoc
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator
from epitator.geoname_annotator import GeonameAnnotator

import re
from datetime import datetime
from geopy import Nominatim
from geopy.extra.rate_limiter import RateLimiter
locator = Nominatim(user_agent="ppcoom")
geocode = RateLimiter(locator.geocode, min_delay_seconds=1/20)
dengue_regex = re.compile(r'([A-Za-z ]+).*\[w\/e (.+)\] \/ (.+) \/ (.+) \/ (.+) \/ (.+) \/ (.+)', re.MULTILINE)

import pandas as pd
from tqdm import tqdm
tqdm.pandas()

# from transformers import BartForConditionalGeneration, BartTokenizer
# # setup our BART transformer summarization model
# print('loading transformers')
# tokenizer = BartTokenizer.from_pretrained('facebook/bart-large-cnn')
# model = BartForConditionalGeneration.from_pretrained('facebook/bart-large-cnn').cuda()

def clean(content):
    split = content.splitlines()
    last_index = -1
    lower = [x.lower().strip() for x in split]
    if '--' in lower:
        last_index = lower.index('--')
    elif 'communicated by:' in lower:
        last_index = lower.index('communicated by:')-1

    cleaned = split[12:last_index]
    return '\n'.join([x for x in cleaned if x])

# helper function to summarize an input text with the BART model
# def summarizer(text: str) -> str:
#     input_ids = tokenizer(text, return_tensors='pt', max_length=1024, padding=True, truncation=True)['input_ids']
#     summary_ids = model.generate(input_ids.cuda())
#     summary = ''.join([tokenizer.decode(s) for s in summary_ids])
#     summary = summary.replace('<s>', '').replace('</s>', '')
#     return summary

# function that extracts location names/admin codes/lat/lng, case and death counts, and date ranges from the input string
# uses epitator since it already trained rules for extracting medical/infectious disease data
def epitator_extract(txt, max_ents=1):
    # input string and add annotators
    doc = AnnoDoc(txt)
    doc.add_tiers(GeonameAnnotator())
    doc.add_tiers(CountAnnotator())
    doc.add_tiers(DateAnnotator())

    # extract geographic data
    geos = doc.tiers["geonames"].spans
    geo_admin1s = [x.geoname.admin1_code for x in geos]
    geo_admin2s = [x.geoname.admin2_code for x in geos]
    geo_admin3s = [x.geoname.admin3_code for x in geos]
    geo_admin4s = [x.geoname.admin4_code for x in geos]
    geo_names = [x.geoname.name for x in geos]
    geo_lats = [x.geoname.latitude for x in geos]
    geo_lons = [x.geoname.longitude for x in geos]

    # extract case counts and death counts
    counts = doc.tiers["counts"].spans
    cases_counts = [x.metadata['count'] for x in counts if 'case' in x.metadata['attributes'] and 'death' not in x.metadata['attributes']]
    cases_tags = [x.metadata['attributes'] for x in counts if 'case' in x.metadata['attributes'] and 'death' not in x.metadata['attributes']]
    death_counts = [x.metadata['count'] for x in counts if 'death' in x.metadata['attributes']]
    death_tags = [x.metadata['attributes'] for x in counts if 'death' in x.metadata['attributes']]

    # extract the date range
    dates = doc.tiers["dates"].spans
    dates_start = [pd.to_datetime(x.metadata["datetime_range"][0], errors='coerce') for x in dates]
    dates_end = [pd.to_datetime(x.metadata["datetime_range"][1], errors='coerce') for x in dates]

    # return only max_ents entities from the extracted lists
    # currently set to the first result for each list, since that is usually the most important one
    # and other ones can be filler/garbage data
    return pd.Series([ 
        geo_admin1s[:max_ents],
        geo_admin2s[:max_ents],
        geo_admin3s[:max_ents],
        geo_admin4s[:max_ents],
        geo_names[:max_ents],
        geo_lats[:max_ents],
        geo_lons[:max_ents],
        cases_counts[:max_ents],
        cases_tags[:max_ents],
        death_counts[:max_ents],
        death_tags[:max_ents],
        dates_start[:max_ents],
        dates_end[:max_ents],
    ])

def parse_dengue(row):
    return pd.DataFrame([(
        *row[2:],
        match[0].strip(),
        pd.NA if re.match(r'\d+ \w{3} \w{4}', re.sub(r'[^\w ]+', '', match[1])) is None 
            else datetime.strptime(re.sub(r'[^\w ]+', '', match[1]), r'%d %b %Y'),
        match[2],
        *[pd.NA if not match[i].isnumeric() else int(match[i].replace(' ', '')) for i in range(3, 7)])
        for match in dengue_regex.findall(row['content'])],
        columns=[
        *row[2:].keys(),
        'location_name',
        'dates_start',
        'serotype',
        'total_cases',
        'confirmed_cases',
        'severe_cases',
        'deaths'
    ])



if __name__ == '__main__':
    print('Opening df')
    df = pd.read_feather('combined_df_anomaly.feather')
    print('Cleaning')
    df['content'] = df['content'].progress_apply(clean)
    df = df[df['content'].str.contains('|'.join(('case', 'cases', 'death', 'deaths')))]

    dengue_df = df[df['disease'] == 'dengue']
    df = df[df['disease'] != 'dengue']

    # df['summary'] = df['content'].progress_apply(summarizer)
    print('Extracting')
    df[['admin1_code',
    'admin2_code',
    'admin3_code',
    'admin4_code',
    'location_name',
    'location_lat',
    'location_lon',
    'cases',
    'cases_tags',
    'deaths',
    'deaths_tags',
    'dates_start',
    'dates_end',
]] = df['content'].progress_apply(epitator_extract)
    df = df.applymap(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    df = df.applymap(lambda y: pd.NA if isinstance(y, (list, str)) and len(y) == 0 else y)
    df = df.reset_index(drop=True)

    dengue_df = pd.concat([parse_dengue(row) for _, row in tqdm(dengue_df.iterrows())])
    dengue_df['location'] = dengue_df['location_name'].progress_apply(geocode)
    dengue_df['point'] = dengue_df['location'].progress_apply(lambda loc: tuple(loc.point) if loc else None)
    dengue_df[['location_lat', 'location_lon', 'altitude']] = pd.DataFrame(dengue_df['point'].tolist(), index=dengue_df.index)
    dengue_df = dengue_df.drop(['location', 'point', 'altitude'], axis=1)
    dengue_df = dengue_df.rename({'confirmed_cases', 'cases'})
    dengue_df = dengue_df.reset_index(drop=True)

    full_df = pd.concat([df, dengue_df], axis=0, ignore_index=True)
    full_df.to_feather('dataset.v1.2.feather')
    full_df = full_df.drop(['Unnamed: 0', 'index'], axis=1)
    full_df.to_feather('dataset.v1.2.feather')