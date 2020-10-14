from setuptools import setup


setup(
    name='powertools',
    version='0.0.1',
    description=(
        "Enables the user to organize transformations of data with "
        "PySpark as a regular Python package."
    ),
    packages=['powertools'],
    python_requires='>=3.7',
    install_requires=[
        'pandas==1.0.4',
        'pyspark==3.0.0',
    ]
)
