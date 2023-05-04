#!/usr/bin/env python3
import asyncio
import os
import traceback
from asyncio import Future, FIRST_COMPLETED
from multiprocessing import Process, Queue
from queue import Empty
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup


async def worker(concurrency: int, root: str, work_q: Queue, result_q: Queue) -> None:
    async with httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=100, max_connections=100, keepalive_expiry=60)) as client:
        async def crawl(url: str) -> set[str]:
            while True:
                try:
                    print(f'{os.getpid()}:{asyncio.current_task().get_name()} processing {url}')
                    html = (await client.get(url, timeout=None)).content
                    soup = BeautifulSoup(html, 'lxml')
                    urls = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
                    return set(filter(lambda u: u.startswith(root) and u.endswith('/'), urls))
                except httpx.HTTPError as e:
                    print(f'Retrying {url}: {str(e)}')

        pending: set[Future] = set()
        try:
            while True:
                try:
                    while len(pending) < concurrency:
                        url = work_q.get_nowait()
                        pending.add(asyncio.ensure_future(crawl(url)))
                except Empty:
                    pass

                if pending:
                    done, pending = await asyncio.wait(pending, timeout=.05, return_when=FIRST_COMPLETED)
                    for future in done:
                        result_q.put(future.result())
                else:
                    await asyncio.sleep(.05)
        except Exception as e:
            traceback.print_exc()


def bootstrap(concurrency: int, root: str, work_q: Queue, result_q: Queue) -> None:
    asyncio.run(worker(concurrency, root, work_q, result_q))


def crawl(concurrency: int, root: str) -> set[str]:
    seen: set[str] = {'http://be.archive.ubuntu.com/ubuntu/ubuntu/'}
    work_q, result_q = Queue(), Queue()

    workers = [Process(target=bootstrap, args=(8, root, work_q, result_q), daemon=True)
               for _ in range(concurrency)]
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
