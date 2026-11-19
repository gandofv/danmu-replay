import asyncio

from replay import experiment


def run(scenario):
    result = asyncio.run(experiment(scenario))
    assert result.published == list(range(1, 11))
    assert result.displayed == result.published, result.report()
    return result


def test_uninterrupted_delivery():
    assert run("normal").sessions == [list(range(1, 11))]


def test_messages_published_while_offline_are_replayed():
    assert run("offline").sessions == [list(range(1, 6)), list(range(6, 11))]


def test_unacknowledged_message_is_redelivered_but_displayed_once():
    assert run("lost-ack").sessions == [list(range(1, 6)), list(range(5, 11))]
    broken = asyncio.run(experiment("lost-ack", deduplicate=False))
    assert broken.displayed == [1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10]
    assert "弹幕 05 重复，位置 5、6" in broken.report()
