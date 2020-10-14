include .python-environment

#################################################################################
# DEVELOPMENT                                                                   #
#################################################################################

## Run all tests. Used to set up a new user. When running: `make` this target will run.
.DEFAULT_GOAL := tests
.PHONY: tests
tests: .venv/bin/activate clean lint
	. .venv/bin/activate; \
		pip install -e tests/sample_packages/squares; \
		pip install -e tests/sample_packages/squares_with_cycle; \
		${PYTHON_INTERPRETER} -m pytest tests --log-cli-level=DEBUG

## Clean every cached file.
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using flake8.
.PHONY: lint
lint: 
	. .venv/bin/activate; \
		${PYTHON_INTERPRETER} -m flake8 powertools
	. .venv/bin/activate; \
		${PYTHON_INTERPRETER} -m flake8 tests

#################################################################################
# ENVIRONMENT                                                                   #
#################################################################################

## Create a virtualironment for DEVELOPMENT.
.venv/bin/activate: .python-environment requirements.txt
	rm -rf .venv
	${PYTHON_INTERPRETER} -c \
		'import sys; assert sys.version_info.major == ${PYTHON_MAJOR_VERSION}'
	${PYTHON_INTERPRETER} -c \
		'import sys; assert sys.version_info.minor >= ${PYTHON_MINOR_VERSION}'
	${PYTHON_INTERPRETER} -m pip install --upgrade setuptools; \
	${PYTHON_INTERPRETER} -m pip install --upgrade wheel; \
	${PYTHON_INTERPRETER} -m pip install --upgrade pip; \
	${PYTHON_INTERPRETER} -m venv .venv
	. .venv/bin/activate; \
		${PYTHON_INTERPRETER} -m pip install -r requirements.txt; \
		${PYTHON_INTERPRETER} -m pip install -e .; \


#################################################################################
# DOCUMENTATION                                                                 #
#################################################################################

.PHONY: docs
docs:
	rm -rf docs
	. .venv/bin/activate; \
		sphinx-apidoc -o _docs/source powertools
	cd _docs; \
		${MAKE} html
	mv _docs/build/html docs
	touch docs/.nojekyll
