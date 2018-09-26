import httpretty
import luigi
import pytest
import requests


from luigi_monitor import luigi_monitor


def test_luigi_monitor_format_message():
    """Test to verify format_message returns as expected"""
    max_print = 1
    job = 'AnyDummyLuigiTask()'
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




# @httpretty.activate
# def test_send_flow_result():
#     url = "https://test-gi-slack-url.com/test/flow/result"
#     max_print = 2
#     status_code = 200
#     httpretty.register_uri(httpretty.GET, url, status=status_code)
#     r = requests.get(url)
#     assert r is not None

def test_send_message():
    pass