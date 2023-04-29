#!/usr/bin/env python3
# Simple singlethreaded web crawler, following all a.href's that end in a '/'.
import sys
from urllib.parse import urljoin
from time import time
from bs4 import BeautifulSoup
import requests

root: str = sys.argv[1] if len(sys.argv) == 2 else 'http://be.archive.ubuntu.com/ubuntu/dists/'


if __name__ == '__main__':
    seen: set[str] = set()
    start = time()
    with requests.Session() as session:
        def crawl(url: str) -> None:
            print(f'processing {url}')
            html = session.get(url).content
            soup = BeautifulSoup(html, 'html.parser')
            paths = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
            for u in filter(lambda p: p.startswith(root) and p.endswith('/'), paths - seen):
                seen.add(u)
                crawl(u)

        crawl(root)

    print(f'{len(seen)} urls crawled in {time() - start:.2f} seconds ({len(seen) / (time() - start):.2f} urls/second)')
