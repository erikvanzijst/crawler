#!/usr/bin/env python3
# Simple multithreaded web crawler, following all a.href's that end in a '/'.
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
from time import time
from threading import current_thread, Barrier, Lock
from bs4 import BeautifulSoup
import requests

inflight = 1


def crawl(concurrency: int, root: str) -> set[str]:
    seen: set[str] = {'http://be.archive.ubuntu.com/ubuntu/ubuntu/'}
    barrier = Barrier(2)
    lock = Lock()

    start = time()
    with ThreadPoolExecutor(concurrency) as executor, requests.Session() as session:
        def crawl(url: str) -> None:
            global inflight
            while True:
                try:
                    print(f'{current_thread().ident} processing {url}')
                    html = session.get(url).content
                    soup = BeautifulSoup(html, 'lxml')
                    paths = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
                    with lock:
                        for u in filter(lambda p: p.startswith(root) and p.endswith('/'), paths - seen):
                            seen.add(u)
                            executor.submit(crawl, u)
                            inflight += 1
                        inflight -= 1
                        if not inflight:
                            barrier.wait()
                    return

                except IOError as e:
                    print(f'Retrying {url} that failed after {time() - start:.2f}s: {str(e)}')

        crawl(root)
        barrier.wait()

    return seen
