# fc-ai-pd12m

A repo forworking with PD12M

## Installation

If the library has been uploaded to Freepik's private artifact registry you need install the following dependencies and authenticate with `gcloud auth login` first:

```shell
pip install keyring keyrings.google-artifactregistry-auth
```

Then install with:

```shell
pip install --extra-index-url https://europe-west1-python.pkg.dev/fc-shared/python/simple/ fc-ai-pd12m
```

It is recommended that you use `uv`, a much faster alternative to `pip` and `pip-tools`. As of `uv` version 0.1.39 you need to:

- `export UV_KEYRING_PROVIDER=subprocess`
- add username `oauth2accesstoken` to the URL [(see why)](https://github.com/astral-sh/uv/blob/49675558eb6498bce2f4b859ad86cf2250059cc6/CHANGELOG.md?plain=1#L138).

```shell
export UV_KEYRING_PROVIDER=subprocess
uv pip install --extra-index-url https://oauth2accesstoken@europe-west1-python.pkg.dev/fc-shared/python/simple/ fc-ai-pd12m
```

To add this library as a dependency to a project, add the following lines to the beginning of your `requirements.txt` file.

```text
# URL to the internal PyPi repository
--extra-index-url https://oauth2accesstoken@europe-west1-python.pkg.dev/fc-shared/python/simple/
```

And this line at the end of the file:

```text
fc-ai-pd12m
```

Finally, you have the option to install directly from the github repo, either via command line:

```shell
uv pip install git+ssh://git@github.com/freepik-company/fc-ai-pd12m.git
```

Similarly, to add to your requirements.txt:

```txt
fc-ai-pd12m @ git+ssh://git@github.com/freepik-company/fc-ai-pd12m.git
```

## Development

### Setting up development environment

The first step is creating a virtual environment:

```shell
uv python install 3.11
uv venv -p 3.11
source .venv/bin/activate
```

Alternatively, with classic python venv (not recommended).

```shell
python -m venv .env
source .env/bin/activate
```

Or, alternatively, with conda (and python 3.10 as an example):

```shell
conda create -n myenv python=3.11
conda activate myenv
```

To set up the development environment, including installing dependencies, pre-commit hooks and the project itself as a library for edition:

````shell
make setup-dev
````

### Adding/modifying a dependency
Inside `pyproject.toml`, edit either `project.dependencies` or `project.optional-dependencies.dev` sections to add a dependency. The latter is for dependencies required only at development time, such as linters, code beautifiers, IDE helpers, etc.

### Commit hooks and linting

It is recommended to run:

```shell
make lint
```

before committing any changes.

Additionally, the make `setup-dev` task installs some pre-commit hooks that
ensures that the quality of the code is up to par before committing.
This is why you may notice some failures when committing. Fix the problems
and commit again.

Sometimes the problem is just the formatter reformatted the code. In
this case, just committing again will fix the problem.

### Running tests

```shell
make test
```

### Update library version

Versioning is automatically managed with `commitizen` every time you make a push to `master` or `main` branches or you close a PR to them as specified in the `fc-pipelines.yaml` file.

For making `commitizen` work, you must follow the conventional commits format style. You can check how to do it [here](https://www.conventionalcommits.org/en/v1.0.0/). The commit message should be structured as follows:

```text
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

For making commits, recommended types are `feat`, `fix`, `build`, `chore`, `ci`, `docs`, `style`, `refactor`, `perf`, `test`.

If you want to update the library version manually, you can run the following command:

```shell
cz bump --changelog
git push
git push --tags
```

This will create a new commit with the updated version, will push it to the remote repository including the new tag.
This will trigger the CI pipeline, which will publish the new version to the internal artifact registry.

### Publishing a new version

[Tekton](https://pipelines.fpkmon.com/#/namespaces/ai-python-libraries/pipelineruns) will take care of publishing a new version of the library as soon as a PR is merged into `master` or `main`.

If you want to publish the library manually, you can run:

```shell
uv build
python -m twine upload dist/* --skip-existing --repository-url https://europe-west1-python.pkg.dev/fc-shared/python/
```

This will upload the new version to the internal artifact registry.
