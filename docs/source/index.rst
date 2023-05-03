.. transformlib documentation master file, created by
   sphinx-quickstart on Wed Oct 14 21:22:04 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

:notoc:

transformlib documentation
======================================

**Documentation Build Date**: |today| **Version**: |release|

**Useful links**:
`Source Repository <https://github.com/laegsgaardTroels/transformlib>`__


Enables the user to organize transformations of data as a regular Python package.

Often one ends up having a lot of main scripts that somehow transform and saves data.

.. highlight:: bash
.. code-block:: bash

    tree transforms
    transforms
    ├── convert_to_json.py
    ├── download_txt.py
    └── produce_reports.py

One of these scripts could look like the example here below.

.. highlight:: python
.. code-block:: python

    from pathlib import Path
    import json

    def main():
        text = Path('mapping.txt').read_text()
        mapping = dict(map(lambda line: line.split(','), text.splitlines()))
        Path('mapping.json').write_text(json.dumps(mapping, indent=4))

    if __name__ == '__main__':
        main()

This package enables the user to organize these scripts in a way that makes them easy to run in
the expected order of execution and easier to read. Below is an example of above main script but
where the :py:func:`transformlib.transform` decorator and :py:class:`transformlib.Output`, :py:class:`transformlib.Input`
class has been used to make the script easier to read.

.. highlight:: python
.. code-block:: python

    import json
    from transformlib import transform, Output, Input


    @transform(
        json_output=Output('mapping.json'),
        txt_input=Input('mapping.txt'),
    )
    def convert_to_json(json_output, txt_input):
        text = txt_input.path.read_text()
        mapping = dict(map(lambda line: line.split(','), text.splitlines()))
        json_output.path.write_text(json.dumps(mapping, indent=4))

Once one has organized the scripts in this way one can run the transformations with an installed command line interface.

.. highlight:: bash
.. code-block:: bash

    transform -h
    transform transforms/*.py
    transform -v transforms/*.py

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`

