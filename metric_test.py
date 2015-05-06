import datetime
import json
import multiprocessing
import sys
import time
import warnings

import requests


# suppress warnings to improve performance
def no_warnings(message, category, filename, lineno):
    pass
warnings.showwarning = no_warnings

num_processes = 10
num_requests = 1000
num_metrics = 200

DISTINCT_METRICS = 20

keystone = {
    'username': 'mini-mon',
    'password': 'password',
    'project': 'test',
    'auth_url': 'http://192.168.10.5:35357/v3'
}

api_url = 'http://192.168.10.4:8080/v2/metrics/'
# api_url = 'http://192.168.10.4:8080/'
# api_url = 'http://127.0.0.1:8080/v2.0/metrics/'

total_metrics = num_processes * num_requests * num_metrics


def agent_sim(num):
    for x in xrange(num_requests):
        body = []
        for i in xrange(num_metrics):
            metric = {"name": "metric_perf",
                      "dimensions": {"dim1": "agent-" + str(num)},
                      "timestamp": time.time() * 1000,
                      "value": 0}
            body.append(metric)

        headers = {'Content-Type': 'application/json'}
        requests.post(url=api_url, data=json.dumps(body), headers=headers)
    print("Done: {}".format(num))


def metric_performance_test():
    start_datetime = datetime.datetime.now()
    start_datetime = start_datetime - datetime.timedelta(microseconds=start_datetime.microsecond)

    process_list = []
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=agent_sim, args=(i,))
        process_list.append(p)

    start_time = time.time()
    print("Starting test at: " + start_datetime.isoformat())
    for p in process_list:
        p.start()

    try:
        for p in process_list:
            try:
                p.join()
            except Exception:
                pass
    except KeyboardInterrupt:
        return False, "User interrupt"

    final_time = time.time()
    print("-----Test Results-----")
    print("{} metrics in {} seconds".format(total_metrics, final_time - start_time))
    print("{} per second".format(total_metrics / (final_time - start_time)))

    return True, ""


def main():
    success, msg = metric_performance_test()
    if not success:
        print("-----Test failed to complete-----")
        print(msg)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
