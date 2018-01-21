import ursa
import pytest
import ray

ray.init()

test_graph_id = "Test Graph"


@pytest.fixture
def init_test():
    manager = ursa.Graph_manager()
    manager.create_graph(test_graph_id)
    return manager


def test_create_graph_bad_name():
    manager = init_test()
    with pytest.raises(ValueError):
        manager.create_graph("")
    with pytest.raises(ValueError):
        manager.create_graph(None)


def test_create_graph_good_name():
    manager = init_test()
    name = "Good name"
    manager.create_graph(name)

    assert name in manager.graph_dict


def test_create_graph_duplicate_name():
    manager = init_test()
    name = "Good name"
    manager.create_graph(name)

    with pytest.raises(ValueError):
        manager.create_graph(name)


def test_insert_bad_input():
    manager = init_test()
    with pytest.raises(ValueError):
        manager.insert(test_graph_id, "Key", "Value", set(), "Bad input")


def test_insert_and_select_roundtrip():
    manager = init_test()
    manager.insert(test_graph_id, "Key1", "Value1")

    row_query1 = manager.select_row(test_graph_id, "Key1")
    assert ray.get(row_query1) == "Value1"

    l_key_query1 = manager.select_local_edges(test_graph_id, "Key1")
    assert ray.get(l_key_query1) == set()

    f_key_query1 = manager.select_foreign_edges(test_graph_id, "Key1")
    assert f_key_query1 == {}

    manager.insert(test_graph_id, "Key2", "Value2", "Key1")

    row_query2 = manager.select_row(test_graph_id, "Key2")
    assert ray.get(row_query2) == "Value2"

    l_key_query2 = manager.select_local_edges(test_graph_id, "Key2")
    assert ray.get(l_key_query2) == set(["Key1"])

    # testing the bi-directionality invariant
    l_key_query1 = manager.select_local_edges(test_graph_id, "Key1")
    assert ray.get(l_key_query1) == set(["Key2"])

    f_key_query2 = manager.select_foreign_edges(test_graph_id, "Key1")
    assert f_key_query2 == {}

    # testing the foreign key functionality
    manager.insert(test_graph_id, "Key3", "Value3",
                   foreign_edges={"Other Graph": "Foreign Key"})

    row_query3 = manager.select_row(test_graph_id, "Key3")
    assert ray.get(row_query3) == "Value3"

    l_key_query3 = manager.select_local_edges(test_graph_id, "Key3")
    assert ray.get(l_key_query3) == set()

    f_key_query3 = manager.select_foreign_edges(test_graph_id, "Key3")
    assert ray.get(f_key_query3["Other Graph"]) == set(["Foreign Key"])


def test_add_local_edges():
    manager = init_test()
    manager.insert(test_graph_id, "Key1", "Value1")
    l_key_query1 = manager.select_local_edges(test_graph_id, "Key1")
    assert ray.get(l_key_query1) == set()

    manager.insert(test_graph_id, "Key2", "Value2")
    l_key_query2 = manager.select_local_edges(test_graph_id, "Key2")
    assert ray.get(l_key_query2) == set()

    manager.add_local_edges(test_graph_id, "Key2", "Key1")

    l_key_query2 = manager.select_local_edges(test_graph_id, "Key2")
    assert ray.get(l_key_query2) == set(["Key1"])

    l_key_query1 = manager.select_local_edges(test_graph_id, "Key1")
    assert ray.get(l_key_query1) == set(["Key2"])


def test_add_foreign_edges():
    manager = init_test()
    manager.insert(test_graph_id, "Key1", "Value1")
    f_key_query1 = manager.select_foreign_edges(test_graph_id, "Key1")
    assert f_key_query1 == {}

    manager.add_foreign_edges(test_graph_id, "Key1", "Other Graph",
                             "Foreign Key")
    f_key_query1 = manager.select_foreign_edges(test_graph_id, "Key1")
    assert ray.get(f_key_query1["Other Graph"]) == set(["Foreign Key"])

    f_key_query2 = manager.select_foreign_edges("Other Graph", "Foreign Key")
    assert ray.get(f_key_query2[test_graph_id]) == set(["Key1"])


def test_split():
    manager = init_test()
    manager.insert(test_graph_id, "Key1", "Value1")
    manager.insert(test_graph_id, "Key2", "Value2")
    manager.split_graph(test_graph_id)

    assert len(manager.graph_dict[test_graph_id]) == 2
    assert ray.get(
        manager.graph_dict[test_graph_id][0].row_exists.remote("Key1", 10))
    assert not ray.get(
        manager.graph_dict[test_graph_id][0].row_exists.remote("Key2", 10))
    assert not ray.get(
        manager.graph_dict[test_graph_id][1].row_exists.remote("Key1", 10))
    assert ray.get(
        manager.graph_dict[test_graph_id][1].row_exists.remote("Key2", 10))


def test_update():
    manager = init_test()
    manager.insert(test_graph_id, "Key3", "Value3")
    manager.insert(test_graph_id, "Key4", "Value4")
    manager.update(test_graph_id, "Key3", "UpdatedValue")

    assert ray.get(manager.select_row(test_graph_id, "Key3")) == "UpdatedValue"


def test_update_no_args():
    manager = init_test()
    manager.insert(test_graph_id, "Key3", "Value3")
    manager.insert(test_graph_id, "Key4", "Value4")

    with pytest.raises(ValueError):
        manager.update(test_graph_id, "Key 3")
