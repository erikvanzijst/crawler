#!/usr/bin/env python3
# Simple multi-processing web crawler, following all a.href's that end in a '/'.

import os
import sys
import traceback
from itertools import chain
from multiprocessing import Pool
from time import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests

CONCURRENCY = 64
session = requests.Session()
root: str = sys.argv[1] if len(sys.argv) == 2 else 'http://be.archive.ubuntu.com/ubuntu/dists/'


def crawl(url: str) -> set[str]:
    while True:
        print(f'{os.getpid()} processing {url}')
        try:
            html = session.get(url).content
            soup = BeautifulSoup(html, 'html.parser')
            urls = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
            return set(filter(lambda u: u.startswith(root) and u.endswith('/'), urls))
        except IOError:
            traceback.print_exc()


if __name__ == '__main__':
    todo: set[str] = {root}
    seen: set[str] = {root, 'http://be.archive.ubuntu.com/ubuntu/ubuntu/'}
    start = time()

    with Pool(processes=CONCURRENCY) as pool:
        while todo:
            todo = set(chain(*pool.map(crawl, todo))) - seen
            seen.update(todo)

    print(f'{len(seen)} urls crawled in {time() - start:.2f} seconds '
          f'({len(seen) / (time() - start):.2f} urls/second with {CONCURRENCY} processes)')
