import httpretty
import json
# import luigi
import pytest

from luigi_monitor import luigi_monitor


@pytest.fixture
def job():
    return 'AnyDummyLuigiTask()'


def test_luigi_monitor_format_message(job):
    """Test to verify format_message returns as expected"""
    max_print = 1
    job_status_report, slack_message_attachment = luigi_monitor.format_message(max_print, job)
    assert job_status_report == "Status report for AnyDummyLuigiTask()"
    assert isinstance(slack_message_attachment, list)
    assert set(slack_message_attachment[0].keys()) == {'text', 'color', 'fields'}


@pytest.fixture
def events():
    """global variable - need to override for test session"""
    events = {}
    luigi_monitor.EVENTS = events
    return events


class DummyOnSuccessLuigiTask(luigi.Task):
    def on_success(self):
        return "This was a successful task!"


def test_luigi_monitor_success_with_on_success_message(events):
    """Test success adds INFO message as expected"""
    task = DummyOnSuccessLuigiTask()
    luigi_monitor.success(task)
    assert events['Success'][0]['task'] == 'DummyOnSuccessLuigiTask() INFO: This was a successful task!'


class DummyWithoutOnSuccessLuigiTask(luigi.Task):
    pass


def test_luigi_monitor_success_without_on_success_message(events):
    """Test success does not add INFO message as expected"""
    task = DummyWithoutOnSuccessLuigiTask()
    luigi_monitor.success(task)
    assert events['Success'][0]['task'] == 'DummyWithoutOnSuccessLuigiTask()'


@pytest.fixture
def slack_url():
    return "https://test-gi-slack-url.com/test/flow/result"


@pytest.fixture
def http_mock():
    httpretty.enable()
    yield httpretty
    httpretty.disable()
    httpretty.reset()


@pytest.fixture
def max_print():
    return 1  # any random int will do


def test_send_flow_result(http_mock, slack_url, job, max_print):
    status_code = 200
    http_mock.register_uri(httpretty.POST, slack_url, status=status_code)
    luigi_monitor.send_flow_result(slack_url, max_print, job)
    request_payload = json.loads(http_mock.last_request().body)
    assert set(request_payload.keys()) == {'text', 'attachments'}

