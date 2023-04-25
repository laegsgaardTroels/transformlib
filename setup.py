import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


setup(
    name='transformlib',
    author='Troels Lægsgaard',
    version='0.2.0',
    description=(
        "Enables the user to organize transformations of data as a regular Python package."
    ),
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/laegsgaardTroels/transformlib",
    package_dir={"": "src"},
    packages=find_packages(
        where='src',
        include=['transformlib*'],
    ),
    python_requires='>=3.9',
    install_requires=[],
    extras_require={
        'dev': [
            'flake8==3.8.3',
            'jinja2==3.0.0',
            'pydata-sphinx-theme',
            'pyspark==3.0.0',
            'pytest-cov',
            'pytest-pep8',
            'pytest==5.3.5',
            'sphinx==3.2.1',
        ]
    },
    entry_points={
        'console_scripts': [
            'transform = transformlib.__main__:main',
        ]
    },
)
