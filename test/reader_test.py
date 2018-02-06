import ray
import ursa

ray.init()

test_graph_id = "Test Graph"


def test_load_from_csv():
    manager = ursa.GraphManager()
    ursa_reader = ursa.io.reader(manager)

    ursa_reader.graph_from_csv(
        test_graph_id, 'test/sample_graph.csv')

    assert(manager.vertex_exists(test_graph_id, 'key1') is True)
    assert(manager.vertex_exists(test_graph_id, 'key2') is True)

    assert(ray.get(manager.select_local_edges(
            test_graph_id, 'key2')) is [{'key1'}])
