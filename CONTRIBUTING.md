# How to develop on this project

tosfs welcomes contributions from the community.

**You need PYTHON3!**

This instructions are for linux base systems. (Linux, MacOS, BSD, etc.)
## Setting up your own fork of this repo.

- On github interface click on `Fork` button.
- Clone your fork of this repo. `git clone git@github.com:YOUR_GIT_USERNAME/tosfs.git`
- Enter the directory `cd tosfs`
- Add upstream repo `git remote add upstream https://github.com/fsspec/tosfs.git`

## Setting up your own virtual environment

Import the source code into your IDE(e.g. `Intellij IDEA`). And choose `poetry` to manage this project.
If this step is successful, the IDE would create a virtual env for this project. 
Check it via `poetry env info -p`.

## Install the project in develop mode

Run `make install` to install the project in develop mode.

## Run the tests to ensure everything is working

Run `make test` to run the tests.

## Create a new branch to work on your contribution

Run `git checkout -b my_contribution`

## Make your changes

Edit the files using your preferred editor. (we recommend VIM or VSCode)

## Format the code

Run `make fmt` to format the code.

## Run the linter

Run `make lint` to run the linter.

## Test your changes

Run `make test` to run the tests.

Ensure code coverage report shows `100%` coverage, add tests to your PR.

## Build the docs locally

Run `make docs` to build the docs.

Ensure your new changes are documented.

## Commit your changes

The tosfs use multiple tools(e.g. `pylint`, `flake8`, `blank`...) to check and keep the code style for the project.
So, before commit changes, we strongly recommand you run `make lint` and `make fmt` to verify or reformat your code.
You can use git hook to run it automatically. Follow this:

```shell
cat > ${PROJECT_HOME}/.git/hooks/pre-commit << 'EOF'
#!/bin/sh

# run commands 'make lint' and 'make fmt' before committing
echo "Running make lint and make fmt before commit..."
make lint
lint_exit_code=$?

make fmt
fmt_exit_code=$?

# if 'make lint' or 'make fmt' failed, cancel commit
if [ $lint_exit_code -ne 0 -o $fmt_exit_code -ne 0 ]; then
  echo "make lint or make fmt failed, aborting commit"
  exit 1
fi

# if 'make lint' and 'make fmt' success, then commit
exit 0

EOF

chmod +x ${PROJECT_HOME}/.git/hooks/pre-commit
```

This project uses [conventional git commit messages](https://www.conventionalcommits.org/en/v1.0.0/).

Example: `fix(package): update core.py arguments`

## Push your changes to your fork

Run `git push origin my_contribution`

## Submit a pull request

On github interface, click on `Pull Request` button.

Wait CI to run and one of the developers will review your PR.
## Makefile utilities

This project comes with a `Makefile` that contains a number of useful utility.

```bash 
❯ make
Usage: make <target>

Targets:
help:             ## Show the help.
show:             ## Show the current environment.
install:          ## Install the project in dev mode.
fmt:              ## Format code using black & isort.
lint:             ## Run pep8, black, mypy linters.
test: lint        ## Run tests and generate coverage report.
watch:            ## Run tests on every change.
clean:            ## Clean unused files.
release:          ## Create a new tag for release.
docs:             ## Build the documentation.
release_wheel:    ## Release wheel for python client.
```

## Making a new release

This project uses [semantic versioning](https://semver.org/) and tags releases with `X.Y.Z`
Every time a new tag is created and pushed to the remote repo, github actions will
automatically create a new release on github and trigger a release on PyPI.

For this to work you need to setup a secret called `PIPY_API_TOKEN` on the project settings>secrets, 
this token can be generated on [pypi.org](https://pypi.org/account/).

To trigger a new release all you need to do is.

1. If you have changes to add to the repo
    * Make your changes following the steps described above.
    * Commit your changes following the [conventional git commit messages](https://www.conventionalcommits.org/en/v1.0.0/).
2. Run the tests to ensure everything is working.
4. Run `make release` to create a new tag and push it to the remote repo.

the `make release` will ask you the version number to create the tag, ex: type `0.1.1` when you are asked.

> **CAUTION**:  The make release will change local changelog files and commit all the unstaged changes you have.
