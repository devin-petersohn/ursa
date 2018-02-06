import ursa
import ray

ray.init()


def test_connected_components():
    test_graph_id = "Test Graph"

    manager = ursa.Graph_manager()
    manager.create_graph(test_graph_id)

    manager.insert(test_graph_id, "Key1", "Value1")
    manager.insert(test_graph_id, "Key2", "Value2")
    manager.add_local_edges(test_graph_id, "Key2", "Key1")

    manager.insert(test_graph_id, "Key3", "Value3", "Key2")
    manager.insert(test_graph_id, "Key4", "Value4", "Key6")

    manager.insert(test_graph_id, "Key5", "Value5", "Key1")
    manager.insert(test_graph_id, "Key6", "Value6", "Key9")
    manager.insert(test_graph_id, "Key7", "Value7")
    manager.insert(test_graph_id, "Key8", "Value8")
    manager.insert(test_graph_id, "Key9", "Value9")
    manager.insert(test_graph_id, "Key10", "Value10", "Key11")

    manager.insert(test_graph_id, "Key11", "Value11", "Key12")

    manager.insert(test_graph_id, "Key12", "Value12", "Key10")

    g = manager.get_graph(test_graph_id)
    conn = g.connected_components.remote()

    correct_output = [{'Key1', 'Key2', 'Key3', 'Key5'},
                      {'Key9', 'Key4', 'Key6'},
                      {'Key7'},
                      {'Key8'},
                      {'Key10', 'Key12', 'Key11'}]

    test_output = ray.get(ray.get(ray.get(conn)[0]))

    for s in test_output:
        assert s in correct_output

    assert len(test_output) == len(correct_output)
