#!/usr/bin/env python3
# Simple singlethreaded web crawler, following all a.href's that end in a '/'.
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests


def crawl(concurrency: int, root: str) -> set[str]:
    seen: set[str] = set()
    with requests.Session() as session:
        def crawl(url: str) -> None:
            while True:
                try:
                    print(f'processing {url}')
                    html = session.get(url).content
                    soup = BeautifulSoup(html, 'html.parser')
                    paths = {urljoin(url, a.get('href')) for a in soup.find_all('a')}
                    for u in filter(lambda p: p.startswith(root) and p.endswith('/'), paths - seen):
                        seen.add(u)
                        crawl(u)
                    break
                except IOError as e:
                    print(f'Retrying {url}: {str(e)}')

        crawl(root)
    return seen
