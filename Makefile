.PHONY: help all setup-dev mypy lint test

help: ## Help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-32s\033[0m %s\n", $$1, $$2}'

all: help

setup-dev:  ## Setup dev environment (deps, commit hooks)
	# Install keyring provider
	uv pip install keyring keyrings.google-artifactregistry-auth
	# Install development dependencies and itself for in-place edition
	uv sync --extra dev --keyring-provider subprocess --extra-index-url https://oauth2accesstoken@europe-west1-python.pkg.dev/fc-shared/python/simple/
	# Git hooks for flake8, black, and isort (see .pre-commit-config.yaml)
	pre-commit install

mypy: ## Run the type checker (mypy)
	mypy src/fc_ai_pd12m tests

lint: mypy ## Code linting (formatting, mypy, etc)
	pre-commit run --all-files

test:  ## Run tests
	pytest src/fc_ai_pd12m tests
