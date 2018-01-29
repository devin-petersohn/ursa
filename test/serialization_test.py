import ursa
import ray

ray.init()


def test_write_rows():
    test_data = "AGCGCTGTAGGGACACTGCAGGGAGGCCTCTGCTGCCCTGCT"
    e1 = ursa.graph.Edge("CTGCAGGGAG", 1, "1")
    e2 = ursa.graph.Edge("TGCAGGGAG", 2, "2")
    e3 = ursa.graph.Edge("CTCTGCT", 3, "3")

    test_row = ursa.graph.graph._GraphRow(test_data,
                                          set([e1, e2]),
                                          {"graph2": set([e2, e3])}, 3)

    dest = ursa.graph.utils.write_row.remote(test_row, "graph1", "node1")

    data = ursa.graph.graph.read_row.remote(dest)
    print("data", ray.get(data))
