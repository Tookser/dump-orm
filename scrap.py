import asyncio
import re
from collections import Counter

import nltk
from nltk.corpus import stopwords
from bs4 import  BeautifulSoup
from textblob import TextBlob
from . import Table
from fields import TextField

nltk.download('stopwords', quiet=True)
stop_words_en = set(stopwords.words('english'))
stop_words_ru = set(stopwords.words('russian'))

import aiohttp

async def fetch_url(session, url, semaphore=None):
    if semaphore:
        async with semaphore:
            async with session.get(url) as response:
                return await response.text()
    else:
        async with session.get(url) as response:
            return await response.text()


class UrlProcessingResults(Table):
    url = TextField(pk=True)
    topkeywords = TextField()
    sentiment = TextField()

async def get_response(urls, concurrent_requests):
    async with aiohttp.ClientSession() as session:
        tasks = []
        sema = asyncio.Semaphore(concurrent_requests)
        for url in urls:
            task = asyncio.ensure_future(fetch_url(session, url, sema))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        for url, req in zip(urls, responses):
            page = BeautifulSoup(req, 'html.parser')
            page_txt = page.text
            tokens_obtained = re.findall(r'\b\w+\b', page_txt.lower())
            blobbed_tokens = TextBlob(' '.join(tokens_obtained))
            sentiment = blobbed_tokens.sentiment.polarity
            tokens_obtained = [token for token in tokens_obtained if token not
                               in stop_words_en and token not in stop_words_ru]
            freq_words = Counter(tokens_obtained)
            result = {}
            for word, count in freq_words.most_common(10):
                result[word] = count
            keys = ','.join(result.keys())

            UrlProcessingResults(url=url, topkeywods=keys, sentiment=str(sentiment))

