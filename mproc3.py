#!/usr/bin/env python3
import os
from multiprocessing import Process, Queue
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def worker(root: str, work_q: Queue, result_q: Queue) -> None:
    with requests.Session() as session:
        while True:
            work = work_q.get()
            results = set[str]()
            print(f'{os.getpid()} processing {len(work)} urls...')
            for url in work:
                while True:
                    try:
                        print(f'{os.getpid()} processing {url}')
                        html = session.get(url).content
                        soup = BeautifulSoup(html, 'lxml')
                        urls = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
                        results.update(filter(lambda u: u.startswith(root) and u.endswith('/'), urls))
                        break
                    except IOError as e:
                        print(f'Retrying {url}: {str(e)}')
            result_q.put(results)


def crawl(concurrency: int, root: str) -> set[str]:
    seen: set[str] = {'http://be.archive.ubuntu.com/ubuntu/ubuntu/'}
    work_q, result_q = Queue(), Queue()

    workers = [Process(target=worker, args=(root, work_q, result_q), daemon=True) for i in range(concurrency)]
    for w in workers:
        w.start()

    work_q.put({root})
    inflight = 1
    while inflight:
        todo = result_q.get() - seen
        seen.update(todo)
        for urls in chunker(list(todo), 50):
            inflight += 1
            work_q.put(urls)
        inflight -= 1

    return seen
