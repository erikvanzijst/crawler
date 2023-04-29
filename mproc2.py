#!/usr/bin/env python3
# Simple multiprocessing web crawler, following all a.href's that end in a '/'.
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Barrier
from urllib.parse import urljoin
from time import time

from bs4 import BeautifulSoup
import requests

root: str = sys.argv[1] if len(sys.argv) == 2 else 'http://be.archive.ubuntu.com/ubuntu/dists/'
CONCURRENCY = 64
session = requests.Session()


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
    seen: set[str] = {'http://be.archive.ubuntu.com/ubuntu/ubuntu/'}
    barrier = Barrier(2)
    inflight = 1

    start = time()
    with ProcessPoolExecutor(CONCURRENCY) as executor:
        def schedule(url: str) -> None:
            def cb(urls):
                global inflight
                for u in urls - seen:
                    inflight += 1
                    schedule(u)
                inflight -= 1
                if not inflight:
                    barrier.wait()

            seen.add(url)
            executor.submit(crawl, url).add_done_callback(lambda future: cb(future.result()))

        schedule(root)
        barrier.wait()

    print(f'{len(seen)} urls crawled in {time() - start:.2f} seconds '
          f'({len(seen) / (time() - start):.2f} urls/second with {CONCURRENCY} processes)')
