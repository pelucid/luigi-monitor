import os
import inspect
import json
import luigi
import requests
from contextlib import contextmanager

EVENTS = {}


def discovered(task, dependency):
    raise NotImplementedError


def missing(task, message=None):
    task = str(task)
    if message:
        task = task + ' INFO: {}'.format(message)
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
    on_success_message = task.on_success()
    task = str(task)

    if on_success_message:
        task = task + " INFO: {}".format(on_success_message)

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


def slack_attachment(text, color):
    """See https://api.slack.com/docs/message-attachments (for our luigi-monitor status messages)"""
    return {
        "text": text,
        "color": color,
        "fields": [{
            "title": None,
            "value": None,
            "short": False
        }]
    }


def format_message(max_print, job):
    job_status_title = "Status report for {}".format(job)
    slack_message_payload_attachments = {"attachments": []}

    if 'Success' in EVENTS:
        success_attachment = get_success_slack_attachment(max_print)
        slack_message_payload_attachments['attachments'].append(success_attachment)

    if 'Failure' in EVENTS:
        failure_attachment = get_failure_slack_attachment(max_print)
        slack_message_payload_attachments['attachments'].append(failure_attachment)

    if 'Missing' in EVENTS:
        missing_attachment = get_missing_dependencies_slack_attachement(max_print)
        slack_message_payload_attachments['attachments'].append(missing_attachment)

    return job_status_title, slack_message_payload_attachments


def get_missing_dependencies_slack_attachement(max_print):
    missing_attachment = slack_attachment("*Tasks with missing dependencies:*", "#439FE0")
    if len(EVENTS['Missing']) > max_print:
        missing_attachment['fields'][0]['value'] = (
            "More than %d tasks with missing dependencies. Please check logs." % max_print)
    else:
        tasks_with_missing_dependencies = []
        for missing in EVENTS['Missing']:
            tasks_with_missing_dependencies.append(missing)
        tasks_with_missing_dependencies = "\n".join(tasks_with_missing_dependencies)
        missing_attachment['fields'][0]['value'] = tasks_with_missing_dependencies
    return missing_attachment


def get_failure_slack_attachment(max_print):
    failure_attachment = slack_attachment("*Failure:*", "danger")
    if len(EVENTS['Failure']) > max_print:
        failure_attachment['fields'][0]['value'] = ("More than %d failures. Please check logs." % max_print)
    else:
        failed_tasks = []
        for failure in EVENTS['Failure']:
            failed_tasks.append("Task: {}; Exception: {}".format(failure['task'], failure['exception']))
        failed_tasks = "\n".join(failed_tasks)
        failure_attachment['fields'][0]['value'] = failed_tasks
    return failure_attachment


def get_success_slack_attachment(max_print):
    success_attachment = slack_attachment("*Successes:*", "good")
    if len(EVENTS['Success']) > max_print:
        success_attachment['fields'][0]['value'] = ("More than %d Successes. Please check logs." % max_print)
    else:
        successful_tasks = []
        for success in EVENTS['Success']:
            successful_tasks.append("Task: {}".format(success['task']))
        successful_tasks = "\n".join(successful_tasks)
        success_attachment['fields'][0]['value'] = successful_tasks
    return success_attachment


def send_flow_result(slack_url, max_print, job):
    text, attachments = format_message(max_print, job)
    payload = {'text': text}
    payload.update(attachments)
    return send_message(slack_url, payload)


def send_validation_warning(slack_url, user, warning, job_name):
    """Send a custom warning to given slack_url from a specific user."""
    payload = {
        'text': '{}\n{}'.format(job_name, warning),
        'username': user.name,
        'icon_emoji': user.icon
    }
    return send_message(slack_url, payload)


def send_message(slack_url, payload):
    if not slack_url:
        print "slack_url not provided. Message will not be sent"
        print payload['text']
        return False

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
        send_flow_result(slack_url, max_print, job_name)
