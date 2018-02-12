import ray
import ursa

ray.init()


def test_load_from_csv():
    manager = ursa.GraphManager()

    graph1_data = 'test/sample_data_file.csv'
    graph2_data = 'test/sample_data_file_l2.csv'
    reln_data = 'test/sample_reln_file.csv'

    ursa.read_csv(manager, 'layer1', graph1_data, 0, None)
    ursa.read_csv(manager, 'layer2', graph2_data, 0, reln_data)

    assert(manager.vertex_exists('layer1', 'k1') is True)
    assert(manager.vertex_exists('layer1', 'k2') is True)
    assert(manager.vertex_exists('layer2', 'k3') is True)
    assert(manager.vertex_exists('layer2', 'k4') is True)

    assert(ray.get(manager.select_local_edges(
                'layer1', 'k1'))[0].take(0) is 'k2')
    assert(ray.get(manager.select_foreign_edges(
                'layer1', 'k1')['layer2']) is {'k3'})
