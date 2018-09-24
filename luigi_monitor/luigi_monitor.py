import os
import inspect
import json
import luigi
import requests
from contextlib import contextmanager

EVENTS = {}

SUCCESS = 'Success'
FAILURE = 'Failure'
MISSING = 'Missing'


class SlackNotifications(object):
    slack_events = [SUCCESS, FAILURE, MISSING]
    events_message_cfg = {
        SUCCESS: {
            'title': 'Successes',
            'color': 'good'
        },
        FAILURE: {
            'title': 'Failures',
            'color': 'danger'
        },
        MISSING: {
            'title': 'Tasks with missing dependencies',
            'color': '#439FE0'
        }
    }

    def __init__(self, raised_events, max_print):
        self.raised_events = raised_events
        self.max_print = max_print

    @property
    def no_raised_events_attachment(self):
        return self._get_event_attachment('No raised events (Success/Failure/Missing) - job not run?', '#f7a70a')

    def get_slack_message_attachments(self):
        attachments = []
        for event in self.slack_events:

            if event in self.raised_events:
                self._create_attachment(event, attachments)
        return {"attachments": attachments}

    def _create_attachment(self, event, attachments):
        event_attachment = self._get_event_attachment(self.events_message_cfg[event]['title'],
                                                      self.events_message_cfg[event]['color'])
        if len(self.raised_events[event]) > self.max_print:
            event_attachment['fields'][0]['value'] = self.max_print_message(event)
        else:
            event_tasks = self.event_task_message(event)
            if event_tasks:
                event_attachment['fields'][0]['value'] = event_tasks
                attachments.append(event_attachment)

    def event_task_message(self, event):
        event_tasks = []
        for task in self.raised_events[event]:

            if event == FAILURE:
                if task['task'] == 'MissingDependencyCompleteTask()':
                    continue
                event_tasks.append("Task: {}; Exception: {}".format(task['task'], task['exception']))

            if event == MISSING:
                event_tasks.append(task)

            if event == SUCCESS:
                event_tasks.append("Task: {}".format(task['task']))

        event_tasks = set(event_tasks)
        event_tasks = "\n".join(event_tasks)
        return event_tasks

    def max_print_message(self, event):
        return "More than {} {}. Please check logs.".format(
            str(self.max_print), self.events_message_cfg[event]['title'].lower())

    def _get_event_attachment(self, event_title, event_color):
        """See https://api.slack.com/docs/message-attachments (for our luigi-monitor status messages)"""
        return {
            "text": "*{}*".format(event_title),
            "color": event_color,
            "fields": [{
                "title": None,
                "value": None,
                "short": False
            }]
        }


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


def format_message(max_print, job):
    job_status_report_title = "Status report for {}".format(job)
    slack_notifications = SlackNotifications(EVENTS, max_print)
    slack_message_attachments = slack_notifications.get_slack_message_attachments()
    return job_status_report_title, slack_message_attachments


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
