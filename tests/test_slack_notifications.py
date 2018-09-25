import pytest
from six import iteritems

from luigi_monitor.luigi_monitor import (SUCCESS,
                                         FAILURE,
                                         MISSING,
                                         SlackNotifications)


@pytest.fixture
def max_print():
    return 10


@pytest.fixture
def slack_notifications(request, max_print):
    return SlackNotifications(request.param, max_print)


@pytest.fixture
def expected_slack_attachments(request):
    attachments = []
    for title, color, value in request.param:
        attachments.append({
            "text": "*{}*".format(title),
            "color": color,
            "fields": [{
                "title": None,
                "value": value,
                "short": False
            }]
        })
    return attachments


@pytest.mark.parametrize('slack_notifications, expected_slack_attachments', [
    # Success slack attachment
    ({SUCCESS: [{'task': 'SuccessLuigiTask()'}]},
     [('Successes', 'good', 'Task: SuccessLuigiTask()')]),
    # Failure slack attachment
    ({FAILURE: [{'task': 'FailureLuigiTask()', 'exception': 'Exception()'}]},
     [('Failures', 'danger', 'Task: FailureLuigiTask(); Exception: Exception()')]),
    # Missing slack attachment
    ({MISSING: ['MissingLuigiTask()']}, [('Tasks with missing dependencies', '#439FE0', 'MissingLuigiTask()')]),
    # Success + Success
    ({SUCCESS: [{'task': 'SuccessLuigiTask()'}, {'task': 'SuccessLuigiTask()'}]},
     [('Successes', 'good', 'Task: SuccessLuigiTask()\nTask: SuccessLuigiTask()')]),
    # Success + Failure
    ({SUCCESS: [{'task': 'SuccessLuigiTask()'}],
      FAILURE: [{'task': 'FailureLuigiTask()', 'exception': 'Exception()'}]},
     [('Successes', 'good', 'Task: SuccessLuigiTask()'),
      ('Failures', 'danger', 'Task: FailureLuigiTask(); Exception: Exception()')]),
    # Success + Success + Failure
    ({SUCCESS: [{'task': 'SuccessLuigiTask()'}, {'task': 'SuccessLuigiTask()'}],
      FAILURE: [{'task': 'FailureLuigiTask()', 'exception': 'Exception()'}]},
     [('Successes', 'good', 'Task: SuccessLuigiTask()\nTask: SuccessLuigiTask()'),
      ('Failures', 'danger', 'Task: FailureLuigiTask(); Exception: Exception()')])
], indirect=['slack_notifications', 'expected_slack_attachments'])
def test_slack_notifications_get_slack_attachments(slack_notifications, expected_slack_attachments):
    actual_slack_attachments = slack_notifications.get_slack_message_attachments()['attachments']
    for actual, expected in zip(actual_slack_attachments, expected_slack_attachments):
        assert_slack_attachments_are_equal(actual, expected)


def assert_slack_attachments_are_equal(actual, expected):
    assert actual['text'] == expected['text']
    assert actual['color'] == expected['color']
    assert actual['fields'][0]['value'] == expected['fields'][0]['value']
    assert actual['fields'][0]['title'] is None
    assert actual['fields'][0]['short'] is False


@pytest.fixture
def raised_events():
    return {SUCCESS: [{'task': 'SuccessLuigiTask()'}, {'task': 'SuccessLuigiTask()'}]}


# Parametrise this to be with and without a secondary event which is below max_print
def test_slack_notifications_get_slack_attachments_exceeding_max_print(raised_events):
    max_print = 1 # since there are two raised events in the fixture
    slack_notifications = SlackNotifications(raised_events, max_print)
    slack_attachment = slack_notifications.get_slack_message_attachments()['attachments']
    assert slack_attachment[0]['text'] == '*Successes*'
    assert slack_attachment[0]['fields'][0]['value'] == 'More than 1 successes. Please check logs.'



def test_slack_notifications_get_slack_attachments_no_raised_events_message():
    pass

