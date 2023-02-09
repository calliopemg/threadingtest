# most of the code here taken from: https://eli.thegreenplace.net/2012/01/16/python-parallelizing-cpu-bound-tasks-with-multiprocessing

import math
import multiprocessing
import platform
import psutil
import threading
import time

class Timer(object):
    def __init__(self, name=None):
        self.name = name

    def __enter__(self):
        self.tstart = time.time()

    def __exit__(self, type, value, traceback):
        if self.name:
            print(f'[{self.name}]', end=' ')
        print(f'Elapsed: {(time.time() - self.tstart):.6f} sec')

def factorize_naive(n):
    """ A naive factorization method. Take integer 'n', return list of
        factors.
    """
    if n < 2:
        return []
    factors = []
    p = 2

    while True:
        if n == 1:
            return factors

        r = n % p
        if r == 0:
            factors.append(p)
            n = n // p
        elif p * p >= n:
            factors.append(n)
            return factors
        elif p > 2:
            # Advance in steps of 2 over odd numbers
            p += 2
        else:
            # If p == 2, get to 3
            p += 1
    assert False, "unreachable"

# Each "factorizer" function returns a dict mapping num -> factors
def serial_factorizer(nums):
    return {n: factorize_naive(n) for n in nums}

def threaded_factorizer(nums, nthreads):
    def worker(nums, outdict):
        """ The worker function, invoked in a thread. 'nums' is a
            list of numbers to factor. The results are placed in
            outdict.
        """
        for n in nums:
            outdict[n] = factorize_naive(n)

    # Each thread will get 'chunksize' nums and its own output dict
    chunksize = int(math.ceil(len(nums) / float(nthreads)))
    threads = []
    outs = [{} for i in range(nthreads)]

    for i in range(nthreads):
        # Create each thread, passing it its chunk of numbers to factor
        # and output dict.
        t = threading.Thread(
                target=worker,
                args=(nums[chunksize * i:chunksize * (i + 1)],
                      outs[i]))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    # Merge all partial output dicts into a single dict and return it
    return {k: v for out_d in outs for k, v in out_d.items()}

def mp_worker(nums, out_q):
    """ The worker function, invoked in a process. 'nums' is a
        list of numbers to factor. The results are placed in
        a dictionary that's pushed to a queue.
    """
    outdict = {}
    for n in nums:
        outdict[n] = factorize_naive(n)
    out_q.put(outdict)

def mp_factorizer(nums, nprocs):

    # Each process will get 'chunksize' nums and a queue to put his out
    # dict into
    out_q = multiprocessing.Queue()
    chunksize = int(math.ceil(len(nums) / float(nprocs)))
    procs = []

    for i in range(nprocs):
        p = multiprocessing.Process(
                target=mp_worker,
                args=(nums[chunksize * i:chunksize * (i + 1)],
                      out_q))
        procs.append(p)
        p.start()

    # Collect all results into a single result dict. We know how many dicts
    # with results to expect.
    resultdict = {}
    for i in range(nprocs):
        resultdict.update(out_q.get())

    # Wait for all worker processes to finish
    for p in procs:
        p.join()

    return resultdict

def get_size(bytes, suffix="B"):
    """
    Scale bytes to its proper format
    e.g:
        1253656 => '1.20MB'
        1253656678 => '1.17GB'
    """
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return f"{bytes:.2f}{unit}{suffix}"
        bytes /= factor

def cpuinfo():
    print(f"Physical CPU cores: {psutil.cpu_count(logical=False)}")
    print(f"Logical CPU cores: {psutil.cpu_count(logical=True)}")

def raminfo():
    svmem = psutil.virtual_memory()
    print(f"Total Memory: {get_size(svmem.total)}")
    print(f"Available Memory: {get_size(svmem.available)}")

if __name__ == '__main__':
    print(f"Python v{platform.python_version()}")
    print()
    print()

    cpuinfo()
    raminfo()
    print()
    print()

    n_nums = 999
    nums = [999999999999]
    for i in range(n_nums):
        nums.append(nums[-1] + 2)

    with Timer('serial       '):
        s_d = serial_factorizer(nums)

    for numparallel in [2, 4, 8]:
        with Timer('threaded (x%s)' % numparallel):
            t_d = threaded_factorizer(nums, numparallel)

    for numparallel in [2, 4, 8]:
        with Timer('mp (x%s)      ' % numparallel):
            m_d = mp_factorizer(nums, numparallel)

    assert s_d == t_d == m_d, "results agree"

