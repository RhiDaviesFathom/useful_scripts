# Useful Scripts

## Background
This has been set up as a centralised personal repo that will contain a collection of useful scripts that I have used 
one and might come in hand again later.

## Commands
### Add columns to an existing csv

This script takes two command line arguments
1) The name of the csv file you want to add the columns to.
2) The name of the output csv file you want to create.


```shell
python3 add_column_to_csv.py
```

### Create or edit a parquet file
Edit the source code before running this, so it is relevant for your use case.
```shell
python3 editing_parquets.py
```

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Install uv using one of the methods described in the [uv docs](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer).

For macOS and Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Set up the pre-commit hooks:

```bash
uv run pre-commit install
```

To update the pre-commit hook versions run `uv run pre-commit autoupdate`.

What other things you need to install the software and how to install them.

```bash
Give examples
```

### Dependency Management

This repo uses `uv` to manage dependencies (see the [uv documentation](https://docs.astral.sh/uv/)).

To add a dependency run `uv add {package}`. Only pin a requirement in here if you need to, pinning is handled automatically by the robots. For example, to add a dependency via the command line (for example, numpy):

```shell
uv add numpy
```

This will update [`pyproject.toml`](pyproject.toml). Make sure that you commit this file. The `pyproject.toml` can be reformatted by running `uv run pyproject-fmt`.

## Usage

A step-by-step series of examples that tell you how to run the application.

To ensure the correct environment is used when running scripts, use:

```bash
uv run path/to/script
```