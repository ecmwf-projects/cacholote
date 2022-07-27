import pytest

from cacholote import extra_stores


@pytest.mark.xfail()
def test_MemcacheStore() -> None:
    store = extra_stores.MemcacheStore()

    store.set("1", "a")

    assert store.get("1") == "a"
    assert store.stats["hit"] == 1

    store.set("2", "b")

    assert store.get("2") == "b"
    assert store.stats["hit"] == 2

    store.set("3", "c", expire=-1)

    assert store.get("3") is None
    assert store.stats["miss"] == 1

    store.clear()

    assert store.get("2") is None
    assert store.stats["hit"] == 0


@pytest.mark.xfail()
def test_DynamoDBStore() -> None:
    store = extra_stores.DynamoDBStore("test_cacholote")

    store.set("1", "a")

    assert store.get("1") == "a"
    assert store.stats["hit"] == 1

    store.set("2", "b")

    assert store.get("2") == "b"
    assert store.stats["hit"] == 2

    store.set("3", "c", expire=-1)

    assert store.get("3") is None
    assert store.stats["miss"] == 1

    store.clear()

    assert store.get("2") is None
    assert store.stats["hit"] == 0


@pytest.mark.xfail()
def test_FirestoreStore() -> None:
    store = extra_stores.FirestoreStore("test_cacholote")

    store.set("1", "a")

    assert store.get("1") == "a"
    assert store.stats["hit"] == 1

    store.set("2", "b")

    assert store.get("2") == "b"
    assert store.stats["hit"] == 2

    store.set("3", "c", expire=-1)

    assert store.get("3") is None
    assert store.stats["miss"] == 1

    store.clear()

    assert store.get("2") is None
    assert store.stats["hit"] == 0
