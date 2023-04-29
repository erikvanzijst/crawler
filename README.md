# Concurrency in Python

Various examples of concurrency in Python.

# Sequential

```
$ python3 st.py http://be.archive.ubuntu.com/ubuntu/dists/
7419 urls crawled in 253.43 seconds (29.27 urls/second)
```


# Multithreading

```
$ python3 mt.py http://be.archive.ubuntu.com/ubuntu/dists/
7419 urls crawled in 53.97 seconds (137.46 urls/second with 64 threads)
```


# Multiprocessing

Using `Pool.map()`:

```
$ python3 mp.py http://be.archive.ubuntu.com/ubuntu/dists/
7420 urls crawled in 55.76 seconds (133.07 urls/second with 64 processes)
```

Using `ProcessPoolExecutor.submit()`:

```
$ python3 mp2.py http://be.archive.ubuntu.com/ubuntu/dists/
7420 urls crawled in 30.81 seconds (240.80 urls/second with 64 processes)
```


# AsyncIO

```
$ ./async.py http://be.archive.ubuntu.com/ubuntu/dists/
7420 urls crawled in 54.18 seconds (136.95 urls/second)
```

```
$ ./async.py http://be.archive.ubuntu.com/
56466 urls crawled in 766.13 seconds (73.70 urls/second)
```


# Asyncio + multiprocessing
