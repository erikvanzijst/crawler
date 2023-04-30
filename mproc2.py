#!/usr/bin/env python3
# Simple multiprocessing web crawler, following all a.href's that end in a '/'.
import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Barrier
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

session = requests.Session()
inflight = 1


def crawl_url(url: str, root: str) -> set[str]:
    while True:
        print(f'{os.getpid()} processing {url}')
        try:
            html = session.get(url).content
            soup = BeautifulSoup(html, 'html.parser')
            urls = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
            return set(filter(lambda u: u.startswith(root) and u.endswith('/'), urls))
        except IOError as e:
            print(f'Retrying {url}: {str(e)}')


def crawl(concurrency: int, root: str) -> set[str]:
    seen: set[str] = {'http://be.archive.ubuntu.com/ubuntu/ubuntu/'}
    barrier = Barrier(2)

    with ProcessPoolExecutor(concurrency) as executor:
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
            executor.submit(crawl_url, url, root).add_done_callback(lambda future: cb(future.result()))

        schedule(root)
        barrier.wait()
    return seen
