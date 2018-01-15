#!/bin/bash

# run flake8
python3 -m flake8

# run pytest for python3 on all test files
python3 -m pytest test/local_manager_test.py
python3 -m pytest test/graph_test.py
python3 -m pytest test/test_connected_components.py

#run pytest for python2 on all test files
python2 -m pytest test/local_manager_test.py
python2 -m pytest test/graph_test.py
python2 -m pytest test/test_connected_components.py
