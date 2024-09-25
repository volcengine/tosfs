.ONESHELL:
ENV_PREFIX=$(shell poetry env info -p 2>/dev/null)/bin/

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
	@echo "test:	         ## Run tests and generate coverage report."
	@echo "test_stability:	 ## Run stability tests."
	@echo "watch:            ## Run tests on every change."
	@echo "clean:            ## Clean unused files."
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
	pip install poetry
	poetry lock
	poetry install --with dev

.PHONY: fmt
fmt:              ## Format code using black & isort.
	poetry run ruff check tosfs/ --fix
	$(ENV_PREFIX)isort tosfs/
	$(ENV_PREFIX)black -l 88 tosfs/

.PHONY: lint
lint:             ## Run pep8, black, mypy linters.
	set -e;
	$(ENV_PREFIX)black -l 88 --check tosfs/
	poetry run ruff check tosfs/
	$(ENV_PREFIX)mypy --ignore-missing-imports tosfs/

.PHONY: test
test:             ## Run tests and generate coverage report.
	$(ENV_PREFIX)pytest -vv -s --cov-config .coveragerc --cov=tosfs -l --tb=short --maxfail=1 tosfs/tests/ --ignore=tosfs/tests/test_stability.py

.PHONY: test_stability
test_stability:             ## Run stability tests.
	$(ENV_PREFIX)pytest -vv -s --cov-config .coveragerc --cov=tosfs -l --tb=short --maxfail=1 tosfs/tests/test_stability.py

.PHONY: watch
watch:            ## Run tests on every change.
	@echo "Make sure you have installed entr, if not please install it firstly ..."
	ls **/**.py | entr $(ENV_PREFIX)pytest -s -vvv -l --tb=long --maxfail=1 tosfs/tests/

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

.PHONY: release
release:          ## Create a new tag for release.
	@echo "WARNING: This operation will create s version tag and push to github"
	@read -p "Version? (provide the next x.y.z semver) : " TAG
	@echo "$${TAG}" > tosfs/VERSION
	@$(ENV_PREFIX)gitchangelog > HISTORY.md
	@git add tosfs/VERSION HISTORY.md
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
	pip install poetry
	poetry build
