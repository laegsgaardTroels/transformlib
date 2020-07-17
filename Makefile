include .python-environment

#################################################################################
# DEVELOPMENT                                                                   #
#################################################################################

## Run all tests. Used to set up a new user. When running: `make` this target will run
.DEFAULT_GOAL := tests
.PHONY: tests
tests: ./bin/activate clean lint
	. ./bin/activate; \
		export ENVIRONMENT=TESTING; \
		${PYTHON_INTERPRETER} -m pytest tests --log-cli-level=DEBUG

## Clean every cached file
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using flake8
.PHONY: lint
lint: 
	. .venv/bin/activate; \
		${PYTHON_INTERPRETER} -m flake8 ${PACKAGE_NAME}
	. .venv/bin/activate; \
		${PYTHON_INTERPRETER} -m flake8 tests

#################################################################################
# ENVIRONMENT                                                                   #
#################################################################################

## Create a virtualironment for DEVELOPMENT.
.venv/bin/activate: .python-environment requirements.txt
	rm -rf .venv
	${PYTHON_INTERPRETER} -c \
		'import sys; assert sys.version_info.major == ${PYTHON_MAJOR_VERSION} and sys.version_info.minor >= ${PYTHON_MINOR_VERSION}'
	${PYTHON_INTERPRETER} -m pip install --upgrade pip
	${PYTHON_INTERPRETER} -m pip install --upgrade setuptools
	${PYTHON_INTERPRETER} -m pip install --upgrade wheel
	${PYTHON_INTERPRETER} -m pip install --upgrade virtualenv
	virtualenv --python ${PYTHON_INTERPRETER} .venv
	. .venv/bin/activate; \
		${PYTHON_INTERPRETER} -m pip install -r requirements.txt; \
		${PYTHON_INTERPRETER} -m pip install -e .; \
