import os
import inspect
import json
import luigi
import requests
from contextlib import contextmanager

EVENTS = {}

def discovered(task, dependency):
    raise NotImplementedError

def missing(task):
    task = str(task)
    if 'Missing' in EVENTS:
        EVENTS['Missing'].append(task)
    else:
        EVENTS['Missing'] = [task]

def present(task):
    raise NotImplementedError

def broken(task, exception):
    raise NotImplementedError

def start(task):
    raise NotImplementedError

def failure(task, exception):
    task = str(task)
    failure = {'task': task, 'exception': str(exception)}
    if 'Failure' in EVENTS:
        EVENTS['Failure'].append(failure)
    else:
        EVENTS['Failure'] = [failure]

def success(task):
    task = str(task)

    EVENTS['Success'] = EVENTS.get('Success', [])
    EVENTS['Success'].append({'task': task})

    if 'Failure' in EVENTS:
        EVENTS['Failure'] = [failure for failure in EVENTS['Failure']
                             if task not in failure['task']]
    if 'Missing' in EVENTS:
        EVENTS['Missing'] = [missing for missing in EVENTS['Missing']
                             if task not in missing]

def processing_time(task, time):
    raise NotImplementedError

event_map = {
    "DEPENDENCY_DISCOVERED": {"function": discovered, "handler": luigi.Event.DEPENDENCY_DISCOVERED},
    "DEPENDENCY_MISSING": {"function": missing, "handler": luigi.Event.DEPENDENCY_MISSING},
    "DEPENDENCY_PRESENT": {"function": present, "handler": luigi.Event.DEPENDENCY_PRESENT},
    "BROKEN_TASK": {"function": broken, "handler": luigi.Event.BROKEN_TASK},
    "START": {"function": start, "handler": luigi.Event.START},
    "FAILURE": {"function": failure, "handler": luigi.Event.FAILURE},
    "SUCCESS": {"function": success, "handler": luigi.Event.SUCCESS},
    "PROCESSING_TIME": {"function": processing_time, "handler": luigi.Event.PROCESSING_TIME}
}

def set_handlers(EVENTS):
    if not isinstance(EVENTS, list):
        raise Exception("EVENTS must be a list")

    for event in EVENTS:
        if not event in event_map:
            raise Exception("{} is not a valid event.".format(event))
        handler = event_map[event]['handler']
        function = event_map[event]['function']
        luigi.Task.event_handler(handler)(function)

def format_message(max_print, job):
    text = ["Status report for {}".format(job)]
    if 'Success' in EVENTS:
        text.append("*Successes:*")
        if len(EVENTS['Success']) > max_print:
            text.append("More than %d Successes. Please check logs." % max_print)
        else:
            for success in EVENTS['Success']:
                text.append("Task: {}".format(success['task']))
    if 'Failure' in EVENTS:
        text.append("*Failures:*")
        if len(EVENTS['Failure']) > max_print:
            text.append("More than %d failures. Please check logs." % max_print)
        else:
            for failure in EVENTS['Failure']:
                text.append("Task: {}; Exception: {}".format(failure['task'], failure['exception']))
    if 'Missing' in EVENTS:
        text.append("*Tasks with missing dependencies:*")
        if len(EVENTS['Missing']) > max_print:
            text.append("More than %d tasks with missing dependencies. Please check logs." % max_print)
        else:
            for missing in EVENTS['Missing']:
                text.append(missing)
    if len(text) == 1:
        text.append("Job ran successfully!")
    text = "\n".join(text)
    return text

def send_message(slack_url, max_print, job):
    text = format_message(max_print, job)
    if not slack_url:
        print "slack_url not provided. Message will not be sent"
        print text
        return False
    payload = {"text": text}
    r = requests.post(slack_url, data=json.dumps(payload))
    if not r.status_code == 200:
        raise Exception(r.text)
    return True

@contextmanager
def monitor(EVENTS=['FAILURE', 'DEPENDENCY_MISSING', 'SUCCESS'],
            slack_url=None, max_print=10,
            job_name=os.path.basename(inspect.stack()[-1][1])):
    if EVENTS:
        h = set_handlers(EVENTS)
    # in luigi 2 the binary raises a sys.exit call
    try:
        yield
    except SystemExit:
        send_message(slack_url, max_print, job_name)
