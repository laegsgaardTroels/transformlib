import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


setup(
    name='powertools',
    version='0.0.1',
    description=(
        "Enables the user to organize transformations of data with "
        "PySpark as a regular Python package."
    ),
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/laegsgaardTroels/powertools",
    packages=['powertools'],
    python_requires='>=3.7',
    install_requires=[
        'pyspark==3.0.0',
    ]
)
