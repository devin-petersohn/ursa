import ursa
import pytest
import ray

ray.init()


@pytest.fixture
def init_test():
    return ursa.graph.Graph.remote(0)


def test_simple_insert():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set()
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)

    assert(ray.get(ray.get(
        graph.select_row.remote(transaction_id, key))[0]) == "Value1")

    assert(ray.get(ray.get(
        graph.select_local_keys.remote(transaction_id, key))[0]) == set())

    assert(ray.get(
        graph.select_foreign_keys.remote(transaction_id, key))[0] == {})


def test_insert_with_local_keys():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set(["Key2", "Key3"])
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)

    assert(ray.get(ray.get(
        graph.select_row.remote(transaction_id, key))[0]) == "Value1")

    assert(ray.get(ray.get(
           graph.select_local_keys.remote(transaction_id, key))[0]) ==
           set(["Key2", "Key3"]))

    assert(ray.get(
        graph.select_foreign_keys.remote(transaction_id, key))[0] == {})


def test_insert_with_foreign_keys():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set()
    foreign_keys = {"Other Graph": "Other Key"}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)

    assert(ray.get(ray.get(
        graph.select_row.remote(transaction_id, key))[0]) == "Value1")

    assert(ray.get(ray.get(
        graph.select_local_keys.remote(transaction_id, key))[0]) == set())

    assert(ray.get(ray.get(
           graph.select_foreign_keys.remote(transaction_id,
                                            key))[0]["Other Graph"]) ==
           set(["Other Key"]))


def test_insert_with_local_and_foreign_keys():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set(["Key2", "Key3"])
    foreign_keys = {"Other Graph": "Other Key"}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)

    assert(ray.get(ray.get(
        graph.select_row.remote(transaction_id, key))[0]) == "Value1")

    assert(ray.get(ray.get(
           graph.select_local_keys.remote(transaction_id, key))[0]) ==
           set(["Key2", "Key3"]))

    assert(ray.get(ray.get(
           graph.select_foreign_keys.remote(transaction_id,
                                            key))[0]["Other Graph"]) ==
           set(["Other Key"]))


def test_add_single_local_key():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set()
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)
    graph.add_local_keys.remote(transaction_id, key, "Key2")

    assert(ray.get(ray.get(
           graph.select_local_keys.remote(transaction_id, key))[0]) ==
           set(["Key2"]))


def test_add_multiple_local_keys():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set()
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)
    graph.add_local_keys.remote(transaction_id, key, "Key2", "Key3", "Key4")

    assert(ray.get(ray.get(
           graph.select_local_keys.remote(transaction_id, key))[0]) ==
           set(["Key2", "Key3", "Key4"]))


def test_add_single_foreign_key():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set()
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)
    graph.add_foreign_keys.remote(
        transaction_id, key, "Other Graph", "Other Key1")

    assert(ray.get(ray.get(
           graph.select_foreign_keys.remote(transaction_id,
                                            key))[0]["Other Graph"]) ==
           set(["Other Key1"]))


def test_add_multiple_foreign_keys():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set()
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)
    graph.add_foreign_keys.remote(
        transaction_id, key, "Other Graph", "Other Key1", "Other Key2",
        "Other Key3")

    assert(ray.get(ray.get(
           graph.select_foreign_keys.remote(transaction_id,
                                            key))[0]["Other Graph"]) ==
           set(["Other Key1", "Other Key2", "Other Key3"]))


def test_delete():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set()
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)

    assert(ray.get(graph.row_exists.remote(key, transaction_id)))
    transaction_id += 1
    graph.delete.remote("Key1", transaction_id)
    assert(ray.get(graph.row_exists.remote(key, transaction_id - 1)))
    assert(not ray.get(graph.row_exists.remote(key, transaction_id)))


def test_split():
    graph = init_test()
    key = "Key1"
    oid = "Value1"
    local_keys = set()
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)

    key = "Key2"
    oid = "Value2"
    local_keys = set()
    foreign_keys = {}
    transaction_id = 0
    graph.insert.remote(key, oid, local_keys, foreign_keys, transaction_id)

    second_graph = ursa.graph.Graph.remote(transaction_id,
                                           graph.split.remote())

    assert ray.get(graph.row_exists.remote("Key1", transaction_id))
    assert not ray.get(second_graph.row_exists.remote("Key1", transaction_id))
    assert not ray.get(graph.row_exists.remote("Key2", transaction_id))
    assert ray.get(second_graph.row_exists.remote("Key2", transaction_id))
