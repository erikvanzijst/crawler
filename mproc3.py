#!/usr/bin/env python3
import os
from multiprocessing import Process, Queue
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

session = requests.Session()


def worker(root: str, work_q: Queue, result_q: Queue) -> None:
    while True:
        url = work_q.get()
        while True:
            try:
                print(f'{os.getpid()} processing {url}')
                html = session.get(url).content
                soup = BeautifulSoup(html, 'lxml')
                urls = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
                result_q.put(set(filter(lambda u: u.startswith(root) and u.endswith('/'), urls)))
                break
            except IOError as e:
                print(f'Retrying {url}: {str(e)}')


def crawl(concurrency: int, root: str) -> set[str]:
    seen: set[str] = {'http://be.archive.ubuntu.com/ubuntu/ubuntu/'}
    work_q, result_q = Queue(), Queue()

    workers = [Process(target=worker, args=(root, work_q, result_q), daemon=True) for i in range(concurrency)]
    for w in workers:
        w.start()

    work_q.put(root)
    inflight = 1
    while inflight:
        urls = result_q.get() - seen
        seen.update(urls)
        for url in urls:
            inflight += 1
            work_q.put(url)
        inflight -= 1

    return seen
