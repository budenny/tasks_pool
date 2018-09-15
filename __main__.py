from threading import Thread, Condition
from Queue import Queue
from multiprocessing import cpu_count
import time


class AsyncTasks(object):
    def __init__(self, num_of_tasks):
        self._queue = Queue()
        self._threads = []
        for _ in range(num_of_tasks):
            t = Thread(target=self._task)
            t.start()
            self._threads.append(t)

    def enqueue(self, task, args):
        self._queue.put(item=(task, args))

    def _task(self):
        while True:
            task, args = self._queue.get()
            if task is None:
                break
            task(*args)

    def stop(self):
        for _ in range(len(self._threads)):
            self._queue.put(item=(None, None))

        for t in self._threads:
            t.join()


NoResult = 0x08091985
Pending = 0x06102014


class TaskPool(object):
    def __init__(self):
        self._tasks = AsyncTasks(cpu_count() * 2)
        self._data_cv = Condition()
        self._results = {}

    def request(self, req):
        with self._data_cv:
            if req not in self._results:
                self._results[req] = Pending
                print 'enqueued', req
                self._tasks.enqueue(task=self._process_request, args=(req,))
            else:
                print 'no need to enqueue', req

    def get(self, req, is_blocking=True):
        with self._data_cv:
            r = self._results.get(req, NoResult)
            if r not in (NoResult, Pending):
                print 'got immediately', req, r
                return r

            if not is_blocking:
                return None

            while self._results.get(req, None) == Pending:
                self._data_cv.wait()
            r = self._results.get(req, None)
            print 'got result', req, r
            return r

    def _process_request(self, req):
        res = self._generate_result(req)
        with self._data_cv:
            self._results[req] = res
            print 'processed', req, res
            self._data_cv.notify_all()

    @staticmethod
    def _generate_result(req):
        time.sleep(0.5)
        return req

    def stop(self):
        self._tasks.stop()


def main():
    tp = TaskPool()
    for i in range(2):
        for r in range(20):
            tp.request(r)

    # uncomment to get results without waiting
    # time.sleep(5)

    getters = []
    for i in range(20, 0, -1):  # intentionally n+1 than requested, to check 'get' w/o 'request'
        t = Thread(target=tp.get, args=(i,))
        t.start()
        getters.append(t)

    for t in getters:
        t.join()

    tp.stop()


if __name__ == '__main__':
    main()
