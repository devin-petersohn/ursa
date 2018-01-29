import ursa
import pytest
import ray

ray.init()


@pytest.fixture
def init_test():
    return ursa.graph.Graph.remote(0, 0)


def test_simple_insert():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)

    assert ray.get(ray.get(
        graph.select_vertex.remote(transaction_id, key))[0]) == "Value1"

    assert ray.get(ray.get(
        graph.select_local_edges.remote(transaction_id, key))[0]) == []

    assert ray.get(
        graph.select_foreign_edges.remote(transaction_id, key))[0] == {}


def test_insert_with_local_edges():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = ["Key2", "Key3"]
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)

    assert ray.get(ray.get(
        graph.select_vertex.remote(transaction_id, key))[0]) == "Value1"

    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final = [obj for l in c for obj in l]
    final.sort()
    assert final == ["Key2", "Key3"]

    assert ray.get(
        graph.select_foreign_edges.remote(transaction_id, key))[0] == {}


def test_insert_with_foreign_edges():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {"Other Graph": "Other Key"}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)

    assert ray.get(ray.get(
        graph.select_vertex.remote(transaction_id, key))[0]) == "Value1"

    assert ray.get(ray.get(
        graph.select_local_edges.remote(transaction_id, key))[0]) == []

    assert ray.get(ray.get(graph.select_foreign_edges.remote(
            transaction_id, key))[0]["Other Graph"]) == set(["Other Key"])


def test_insert_with_local_and_foreign_edges():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set(["Key2", "Key3"])
    foreign_edges = {"Other Graph": "Other Key"}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)

    assert ray.get(ray.get(
        graph.select_vertex.remote(transaction_id, key))[0]) == "Value1"

    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final = [obj for l in c for obj in l]
    final.sort()
    assert final == ["Key2", "Key3"]

    assert ray.get(ray.get(graph.select_foreign_edges.remote(
            transaction_id, key))[0]["Other Graph"]) == set(["Other Key"])


def test_add_single_local_key():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)
    graph.add_local_edges.remote(transaction_id, key, "Key2")

    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final = [obj for l in c for obj in l]
    assert final == ["Key2"]


def test_add_multiple_local_edges():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)
    graph.add_local_edges.remote(transaction_id, key, "Key2", "Key3", "Key4")

    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final = [obj for l in c for obj in l]
    final.sort()
    assert final == ["Key2", "Key3", "Key4"]


def test_add_single_foreign_edge():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)
    graph.add_foreign_edges.remote(
        transaction_id, key, "Other Graph", "Other Key1")

    assert ray.get(ray.get(graph.select_foreign_edges.remote(
            transaction_id, key))[0]["Other Graph"]) == set(["Other Key1"])


def test_add_multiple_foreign_edges():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)
    graph.add_foreign_edges.remote(
        transaction_id, key, "Other Graph", "Other Key1", "Other Key2",
        "Other Key3")

    assert ray.get(ray.get(graph.select_foreign_edges.remote(
            transaction_id, key))[0]["Other Graph"]
            ) == set(["Other Key1", "Other Key2", "Other Key3"])


def test_delete():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)

    assert ray.get(graph.vertex_exists.remote(key, transaction_id))
    transaction_id += 1
    graph.delete.remote("Key1", transaction_id)
    assert ray.get(graph.vertex_exists.remote(key, transaction_id - 1))
    assert not ray.get(graph.vertex_exists.remote(key, transaction_id))


def test_split():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)

    key = "Key2"
    vertex_data = "Value2"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)

    second_graph = ursa.graph.Graph.remote(transaction_id,
                                           key,
                                           rows=graph.split.remote())

    assert ray.get(graph.vertex_exists.remote("Key1", transaction_id))
    assert not \
        ray.get(second_graph.vertex_exists.remote("Key1", transaction_id))
    assert not ray.get(graph.vertex_exists.remote("Key2", transaction_id))
    assert ray.get(second_graph.vertex_exists.remote("Key2", transaction_id))


def test_update_deleted_vertex():
    graph = init_test()
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote("Key3", "Value3", local_edges, foreign_edges,
                        transaction_id)
    graph.insert.remote("Key4", "Value4", local_edges, foreign_edges,
                        transaction_id)

    graph.delete.remote("Key3", transaction_id)
    graph.update.remote("Key3", "UpdatedValue", local_edges, foreign_edges,
                        transaction_id)

    assert "Key3" not in ray.get(graph.select_vertex.remote(transaction_id))


def test_non_existant_vertex():
    graph = init_test()
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote("Key3", "Value3", local_edges, foreign_edges,
                        transaction_id)
    graph.insert.remote("Key4", "Value4", local_edges, foreign_edges,
                        transaction_id)

    graph.delete.remote("Key3", transaction_id)
    graph.update.remote("Key9999", "UpdatedValue", local_edges, foreign_edges,
                        transaction_id)

    assert "Key9999" not in ray.get(graph.select_vertex.remote(transaction_id))
