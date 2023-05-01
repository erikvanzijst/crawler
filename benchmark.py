#!/usr/bin/env python3
#
import subprocess

if __name__ == '__main__':
    for concurrency in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 20, 24, 28, 32, 36, 40, 44, 48, 64]:
        for prog in ['mthreading', 'async', 'mproc2', 'sequential']:
            print(f'Running {prog} with concurrency {concurrency}...')
            subprocess.check_call(['python3', 'crawl.py', '-p', prog, '-c', str(concurrency),
                                   'http://be.archive.ubuntu.com/ubuntu/'])
