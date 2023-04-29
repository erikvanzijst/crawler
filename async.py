#!/usr/bin/env python3
# Simple syncio web crawler, following all a.href's that end in a '/'.

import asyncio
import sys
from urllib.parse import urljoin
from time import time

import httpx
from bs4 import BeautifulSoup

CONCURRENCY = 64
root: str = sys.argv[1] if len(sys.argv) == 2 else 'http://be.archive.ubuntu.com/ubuntu/dists/'


if __name__ == '__main__':
    seen: set[str] = {'http://be.archive.ubuntu.com/ubuntu/ubuntu/'}
    q = asyncio.Queue()
    barrier = asyncio.Barrier(2)    # Python 3.11 or higher required
    inflight = 1

    async def crawl(client: httpx.AsyncClient) -> None:
        global inflight

        while inflight:
            url = await q.get()
            while True:
                start = time()
                try:
                    print(f'{asyncio.current_task().get_name()} processing {url}')
                    html = (await client.get(url, timeout=None)).content
                    soup = BeautifulSoup(html, 'html.parser')
                    paths = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
                    for u in filter(lambda p: p.startswith(root) and p.endswith('/'), paths - seen):
                        await q.put(u)
                        inflight += 1
                        seen.add(u)
                    break

                except httpx.HTTPError as e:
                    print(f'Retrying {url} that failed after {time() - start:.2f}s: {str(e)}')
            inflight -= 1
        await barrier.wait()

    async def main() -> None:
        await q.put(root)
        async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=60, max_connections=1000)) as client:
            for i in range(CONCURRENCY):
                asyncio.create_task(crawl(client), name=f'{i:03d}')
            await barrier.wait()

    start = time()
    asyncio.run(main())
    print(f'{len(seen)} urls crawled in {time() - start:.2f} seconds '
          f'({len(seen) / (time() - start):.2f} urls/second with {CONCURRENCY} coroutines)')
