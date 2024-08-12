.ONESHELL:
ENV_PREFIX=$(shell poetry env info -p)/bin/
TEST_DIR?="tests/"
BENCHMARK?="benchmark/"

.PHONY: help
help:             ## Show the help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "help:             ## Show the help."
	@echo "show:             ## Show the current environment."
	@echo "install:          ## Install the project in dev mode."
	@echo "fmt:              ## Format code using black & isort."
	@echo "lint:             ## Run pep8, black, mypy linters."
	@echo "test: lint        ## Run tests and generate coverage report."
	@echo "watch:            ## Run tests on every change."
	@echo "clean:            ## Clean unused files."
	@echo "virtualenv:       ## Create a virtual environment."
	@echo "release:          ## Create a new tag for release."
	@echo "docs:             ## Build the documentation."
	@echo "release_wheel:    ## Release wheel for python client."

.PHONY: show
show:             ## Show the current environment.
	@echo "Current environment:"
	@echo "Running using $(ENV_PREFIX)"
	@$(ENV_PREFIX)python -V
	@$(ENV_PREFIX)python -m site

.PHONY: install
install:          ## Install the project in dev mode.
	@echo "Don't forget to run 'make virtualenv' if you got errors."
	$(ENV_PREFIX)pip install fsspec==2023.5.0 -e .[test]

.PHONY: fmt
fmt:              ## Format code using black & isort.
	$(ENV_PREFIX)isort pyproton/
	$(ENV_PREFIX)black -l 79 pyproton/
	$(ENV_PREFIX)black -l 79 tests/
	$(ENV_PREFIX)black -l 79 benchmark/

.PHONY: lint
lint:             ## Run pep8, black, mypy linters.
	set -e;
	$(ENV_PREFIX)flake8 pyproton/
	$(ENV_PREFIX)black -l 79 --check pyproton/
	$(ENV_PREFIX)black -l 79 --check tests/
	$(ENV_PREFIX)black -l 79 --check benchmark/
	$(ENV_PREFIX)mypy --ignore-missing-imports pyproton/

.PHONY: test
test:             ## Run tests and generate coverage report.
	$(ENV_PREFIX)pytest -v -s --cov-config .coveragerc --cov=pyproton -l --tb=short --maxfail=1 ${TEST_DIR}

.PHONY: watch
watch:            ## Run tests on every change.
	@echo "Make sure you have installed entr, if not please install it firstly ..."
	ls **/**.py | entr $(ENV_PREFIX)pytest -s -vvv -l --tb=long --maxfail=1 tests/

.PHONY: clean
clean:            ## Clean unused files.
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '__pycache__' -exec rm -rf {} \;
	@find ./ -name 'Thumbs.db' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -rf htmlcov
	@rm -rf .tox/
	@rm -rf docs/_build

.PHONY: virtualenv
virtualenv:       ## Create a virtual environment.
	@echo "creating virtualenv ..."
	@rm -rf .venv
	@python3 -m venv .venv
	@./.venv/bin/pip install -U pip
	@./.venv/bin/pip install -e .[test]
	@echo
	@echo "!!! Please run 'source .venv/bin/activate' to enable the environment !!!"

.PHONY: release
release:          ## Create a new tag for release.
	@echo "WARNING: This operation will create s version tag and push to github"
	@read -p "Version? (provide the next x.y.z semver) : " TAG
	@echo "$${TAG}" > pyproton/VERSION
	@$(ENV_PREFIX)gitchangelog > HISTORY.md
	@git add pyproton/VERSION HISTORY.md
	@git commit -m "release: version $${TAG} 🚀"
	@echo "creating git tag : $${TAG}"
	@git tag $${TAG}
	@git push -u origin HEAD --tags
	@echo "Github Actions will detect the new tag and release the new version."

.PHONY: docs
docs:             ## Build the documentation.
	@echo "building documentation ..."
	@$(ENV_PREFIX)mkdocs build
	URL="site/index.html"; xdg-open $$URL || sensible-browser $$URL || x-www-browser $$URL || gnome-open $$URL || open $$URL

.PHONY: release_wheel
release_wheel:      ## Release wheel for python client.
	@echo "Releasing wheel for python client ..."
	@$(ENV_PREFIX)pip install setuptools wheel twine
	@$(ENV_PREFIX)python setup.py sdist bdist_wheel

.PHONY: benchmark
benchmark:         ## Run benchmark tests.
	@echo "Running benchmark ..."
    ifeq ($(TYPE),read)
	    @echo "Benchmark type: $(TYPE)"
	    $(ENV_PREFIX)pytest -v -s --tb=short -l -k "read" --maxfail=1 --benchmark-only $(BENCHMARK)
    else ifeq ($(TYPE),write)
	    @echo "Benchmark type: $(TYPE)"
	    $(ENV_PREFIX)pytest -v -s --tb=short -l -k "write" --maxfail=1 --benchmark-only $(BENCHMARK)
    else ifeq ($(TYPE),rm)
	    @echo "Benchmark type: $(TYPE)"
	    $(ENV_PREFIX)pytest -v -s --tb=short -l -k "rm" --maxfail=1 --benchmark-only $(BENCHMARK)
    else
	    @echo "Did not specify the effective 'TYPE' option, will run all the benchmarks"
	    $(ENV_PREFIX)pytest -v -s -l --tb=short --maxfail=1 --benchmark-only $(BENCHMARK)
    endif
