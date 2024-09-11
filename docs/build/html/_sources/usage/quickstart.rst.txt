Quickstart
==========

Assume one wants to build a Machine Learning model for the California housing dataset. The recommended folder structure will then be::

 .
 ├── data
 │   └── california_housing.csv
 ├── pipeline.py
 ├── pipeline.sh
 └── transforms
     ├── download.py
     ├── eval.py
     ├── split.py
     ├── train.py
     └── tune.py

The ``transforms/`` folder contains all the transformation applied to the data. To run the transforms one can use the installed Command Line Inferface (CLI):

.. highlight:: bash
.. code-block:: bash

    transform transforms/*.py -d data -v

This will topologically sort and run all :py:class:`transformlib.Transform` objects found in the .py files found at ``transforms/*.py`` and load/save the results to the data directory ``data/``. For all the options see:

.. highlight:: bash
.. code-block:: bash

    transform -h

For more see the `california housing example <https://github.com/laegsgaardTroels/transformlib/tree/master/examples/california_housing>`__.
