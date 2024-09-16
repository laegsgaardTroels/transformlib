import pathlib
import re
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# A file containing the __version__ variable.
VERSIONFILE = "src/transformlib/__init__.py"

try:
    __version__ = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]", open(VERSIONFILE,
                                                    "rt").read(), re.M
    ).group(1)
except Exception as exception:
    raise RuntimeError(
        f"Unable to find version string in {VERSIONFILE}") from exception


setup(
    name="transformlib",
    author="Troels Lægsgaard",
    version=__version__,
    description=(
        "Enables the user to organize transformations of data as a regular Python package."
    ),
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/laegsgaardTroels/transformlib",
    package_dir={"": "src"},
    packages=find_packages(
        where="src",
        include=["transformlib*"],
    ),
    python_requires=">=3.10",
    install_requires=[],
    extras_require={
        "all": [
            "pandas>=2.2.1",
        ],
        "pandas": [
            "pandas>=2.2.1",
        ],
        "dev": [
            "flake8==3.8.3",
            "jinja2==3.0.0",
            "pytest-cov",
            "pytest-pep8",
            "pytest==8.0.2",
            "sphinx==7.2.6",
            "scikit-learn==1.4.1.post1",
            "pandas==2.2.1",
        ],
    },
    entry_points={
        "console_scripts": [
            "transform = transformlib.__main__:main",
        ]
    },
)
