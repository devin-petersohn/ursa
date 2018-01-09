#!/bin/bash

# run flake8
flake8 . 

# run pytest for python3 on all test files
python3 -m pytest test/local_manager_test.py
# python3 -m pytest test/graph_test.py

#run pytest for python2 on all test files
python2 -m pytest test/local_manager_test.py
# python2 -m pytest test/graph_test.py
