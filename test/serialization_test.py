import ursa
import ray

ray.init()


def test_write_vertices():
    test_data = "AGCGCTGTAGGGACACTGCAGGGAGGCCTCTGCTGCCCTGCT"
    e1 = ursa.graph.Edge("CTGCAGGGAG", 1, "1")
    e2 = ursa.graph.Edge("TGCAGGGAG", 2, "2")
    e3 = ursa.graph.Edge("CTCTGCT", 3, "3")

    test_row = ursa.graph.vertex._Vertex(test_data,
                                         set([e1, e2]),
                                         {"graph2": set([e2, e3])})

    dest = "graph1/node1/-1.dat"
    ursa.graph.utils.write_vertex.remote(test_row, "graph1", "node1")

    oid = ursa.graph.utils.read_vertex.remote(dest)
    data = ray.get(oid)

    assert(type(data) is ursa.graph.vertex._Vertex)
