import ursa
import pytest
import ray
from test_utils import flatten

ray.init()


@pytest.fixture
def init_test():
    return ursa.graph.Graph.remote(0)


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
    final = flatten(c)
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
    local_edges = ["Key2", "Key3"]
    foreign_edges = {"Other Graph": "Other Key"}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)

    assert ray.get(ray.get(
        graph.select_vertex.remote(transaction_id, key))[0]) == "Value1"

    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final = flatten(c)
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
    final = flatten(c)
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
    graph.add_local_edges.remote(transaction_id, key, "Key02", "Key03",
                                 "Key04", "Key05", "Key06", "Key07", "Key08",
                                 "Key09", "Key10", "Key11", "Key12", "Key13")

    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final = flatten(c)
    final.sort()
    assert final == ["Key02", "Key03", "Key04", "Key05", "Key06", "Key07",
                     "Key08", "Key09", "Key10", "Key11", "Key12", "Key13"]


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
                                           vertices=graph.split.remote())

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


def test_clean_buffered_local_edges():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)
    graph.add_local_edges.remote(transaction_id, key,
                                 "Key5", "Key3", "Key4", "Key2")

    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final = flatten(c)
    assert final == ["Key5", "Key3", "Key4", "Key2"]
    graph.clean_local_edges.remote("Key1")
    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final_sorted = flatten(c)
    assert final_sorted == ["Key2", "Key3", "Key4", "Key5"]


def test_clean_flushed_local_edges():
    graph = init_test()
    key = "Key1"
    vertex_data = "Value1"
    local_edges = set()
    foreign_edges = {}
    transaction_id = 0
    graph.insert.remote(key, vertex_data, local_edges, foreign_edges,
                        transaction_id)
    graph.add_local_edges.remote(transaction_id, key, "Key02", "Key03",
                                 "Key14", "Key06", "Key07", "Key18",
                                 "Key19", "Key10", "Key12", "Key11")
    graph.add_local_edges.remote(transaction_id, key, "Key15", "Key13",
                                 "Key05", "Key04", "Key09", "Key08",
                                 "Key24", "Key21", "Key17", "Key20", "Key16")
    graph.add_local_edges.remote(transaction_id, key, "Key23", "Key22")

    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    c = ray.get(r[0])
    c.extend(r[1])
    final = flatten(c)
    assert final == ["Key02", "Key03", "Key06", "Key07", "Key10",
                     "Key11", "Key12", "Key14", "Key18", "Key19", "Key04",
                     "Key05", "Key08", "Key09", "Key13", "Key15",
                     "Key16", "Key17", "Key20", "Key21", "Key24", "Key23",
                     "Key22"]
    graph.clean_local_edges.remote("Key1")
    r = ray.get(graph.select_local_edges.remote(transaction_id, key))
    # print r
    c = ray.get(r[0])
    c.extend(r[1])
    # print c
    final_sorted = flatten(c)
    # print final_sorted
    assert final_sorted == ["Key02", "Key03", "Key04", "Key05", "Key06",
                            "Key07", "Key08", "Key09", "Key10", "Key11",
                            "Key12", "Key13", "Key14", "Key15", "Key16",
                            "Key17", "Key18", "Key19", "Key20", "Key21",
                            "Key24", "Key23", "Key22"]
