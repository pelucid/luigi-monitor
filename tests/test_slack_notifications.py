import pytest
from six import iteritems

from luigi_monitor.luigi_monitor import (SUCCESS,
                                         FAILURE,
                                         MISSING,
                                         SlackNotifications)


@pytest.fixture
def max_print():
    """The max number of tasks to print out before deciding to print out a summary message
    Example:
        Given 3 successful tasks

        when max_print is set to 3
            Successful tasks:
            Task1
            Task2
            Task3

        when max_print is set to 2
            More than 2 successful tasks!
            """
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
    ({SUCCESS: [{'task': 'SuccessLuigiTask()'}]}, [('Successes', 'good', 'Task: SuccessLuigiTask()')]),
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
    """Test to verify slack_attachments are returned correctly for respective events"""
    actual_slack_attachments = slack_notifications.get_slack_message_attachments()['attachments']
    for actual, expected in zip(actual_slack_attachments, expected_slack_attachments):
        assert_slack_attachments_are_equal(actual, expected)


def assert_slack_attachments_are_equal(actual, expected):
    assert actual['text'] == expected['text']
    assert actual['color'] == expected['color']
    assert actual['fields'][0]['value'] == expected['fields'][0]['value']
    assert actual['fields'][0]['title'] is None
    assert actual['fields'][0]['short'] is False


def test_slack_notifications_get_slack_attachments_exceeding_max_print():
    """Test to verify that correct slack message text is returned when max_print is exceeded"""
    raised_events = {SUCCESS: [{'task': 'SuccessLuigiTask()'}, {'task': 'SuccessLuigiTask()'}]}
    max_print = 1  # since there are two raised events (two dicts in list above)
    slack_notifications = SlackNotifications(raised_events, max_print)
    slack_attachments = slack_notifications.get_slack_message_attachments()['attachments']
    assert slack_attachments[0]['text'] == '*Successes*'
    assert slack_attachments[0]['fields'][0]['value'] == 'More than 1 successes. Please check logs.'


def test_slack_notifications_get_slack_attachments_no_raised_events_message():
    """Test to verify that correct slack message text is returned when there are no raised events - i.e. job not run"""
    raised_events = {}
    max_print = 1
    slack_notifications = SlackNotifications(raised_events, max_print)
    slack_attachments = slack_notifications.get_slack_message_attachments()['attachments']
    assert slack_attachments[0]['text'] == '*No raised events (Success/Failure/Missing)* - ' \
                                           'possibly:\n  - job not run\n  - job already run' \
                                           '\n... No new data to process?'
