#!/usr/bin/env python3
# Web crawler cli.

import argparse
import importlib
import os
import sys
from time import time

import pandas


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Web crawler')
    parser.add_argument('--prog', '-p', type=str, default='sequential', help='Program to run')
    parser.add_argument('--concurrency', '-c', type=int, default=48, help='Number of coroutines')
    parser.add_argument('root', default='http://be.archive.ubuntu.com/ubuntu/dists/', nargs='?', help='Root URL to crawl')
    args = parser.parse_args()

    start = time()

    crawler = importlib.import_module(f'{args.prog}')
    print(f'Crawling {args.root} ({args.prog} with concurrency {args.concurrency})...')
    seen = crawler.crawl(args.concurrency, args.root)

    print(f'{len(seen)} urls crawled in {time() - start:.2f} seconds '
          f'({len(seen) / (time() - start):.2f} urls/second with {args.concurrency} workers)')

    # Write results to csv file
    df = pandas.DataFrame({'prog': [args.prog],
                           'python': [f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}'],
                           'root': [args.root], 'concurrency': [args.concurrency], 'urls': [len(seen)],
                           'time': [time() - start],
                           'parser': ['lxml']})
    if os.path.exists('results.csv'):
        df = pandas.concat([pandas.read_csv('results.csv'), df])
    df.to_csv('results.csv', index=False)
