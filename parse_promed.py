import re
import sys
sys.path.append('../EpiTator')

import pandas as pd

from tqdm import tqdm
tqdm.pandas()

from epitator.annotator import AnnoDoc
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator
from epitator.geoname_annotator import GeonameAnnotator

from transformers import BartForConditionalGeneration, BartTokenizer
from typing import List

# setup our BART transformer summarization model
tokenizer = BartTokenizer.from_pretrained('facebook/bart-large-cnn')
model = BartForConditionalGeneration.from_pretrained('facebook/bart-large-cnn').cuda()

# helper function to summarize an input text with the BART model
def summarizer(text: str) -> str:
    input_ids = tokenizer(text, return_tensors='pt', max_length=1024, padding=True, truncation=True)['input_ids']
    summary_ids = model.generate(input_ids.cuda())
    summary = ''.join([tokenizer.decode(s) for s in summary_ids])
    summary = summary.replace('<s>', '').replace('</s>', '')
    return summary

# helper function to strip html tags from a string (needed for better accuracy)
def clean_html(raw_html: str, strip=True) -> str:
  cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
  clean = re.sub(cleanr, '', raw_html)
  if strip:
    clean = re.sub(r'\W+', ' ', clean)
  return clean