import csv


def read_csv(self, graph_manager, label, data_file, key_col, reln_file=None):
    """
    Generates a graph from node data CSV file and optionally supplied
    relationship CSV file to define edges

    Keyword arguments:
    @param label: Name of the new graph
    @param data_file: Path to the CSV file with graph data
    @param key_col: Index or column name in data file
    @param reln_file: Path to the optional CSV file with reln data
    """

    graph_id = label

    # Load raw CSV data into an array to process
    raw_vertex_data = []
    with open(data_file) as csv_file:
        graph_reader = csv.reader(csv_file)
        for row in graph_reader:
            raw_vertex_data.append(row)

    # Insert vertices with data
    # (TODO) Check if column titles provided, right now assuming yes
    column_titles = raw_vertex_data[0]
    raw_vertex_data = raw_vertex_data[1:]

    if type(key_col) == str:
        key_col = column_titles.index(key_col)

    for row in raw_vertex_data:
        node = {}
        for col, val in enumerate(row):
            if col == key_col:
                key = val
            else:
                node[column_titles[col]] = val

        self.graph_manager.insert(graph_id, key, node)

    # Insert edges from relationship file
    # (TODO) Check if column titles provided, right now assuming yes
    # Assume fmt src_label, src_node, dest_label, dest_node
    if reln_file is not None:
        raw_edge_data = []
        with open(reln_file) as csv_file:
            graph_reader = csv.reader(csv_file)
            for row in graph_reader:
                raw_edge_data.append(row)

        column_titles = raw_edge_data[0]
        raw_edge_data = raw_edge_data[1:]

        for edge in raw_edge_data:
            src_graph_label, src_graph_key = edge[0], edge[1]
            dest_graph_label, dest_graph_key = edge[2], edge[3]

            # Insert local edge
            if src_graph_label == dest_graph_label:
                self.graph_manager.add_local_edges(
                    src_graph_label,
                    src_graph_key,
                    dest_graph_key)

            # Insert foreign edge
            else:
                self.graph_manager.add_foreign_edges(
                    src_graph_label,
                    src_graph_key,
                    dest_graph_label,
                    dest_graph_key)
