# Python-Starter

An example repo to help set up new python projects. The following files and directories are optional and can be deleted
 depending on your project:
1. notebooks

If the repository is simple and only contains one main package, rename the `src`
repository to the name of the main package. i.e `does_it_flood`.

A README.md should include the following information:

## Background

## Commands

The central entry points for common tasks related to this project.

## Installation/ Getting Started

This is where you describe how to get set up on a clean install, including the
commands necessary to get the raw data, and then how to make the cleaned, final data sets.

Be conscious that people use different operating systems and use pip or Conda. Be verbose in your install instructions
so that anyone can spin up your project

# Python Starter How to use

**NOTE:** This is not ready for prime time and is 100% a work in progress! Treat with care.

The point of this lil repo is to provide a starting point for Python projects at Fathom. The example in this repo is a (very) simple script whose interface should be applicable to any Python script.

## Environment Setup

This repo contains example Pip and Conda environment files. This is purely to provide an example of both tools! It is strongly advised to try to use only one of the other. The best approach for science-y / data processing projects is to start with Conda and only install via Pip if a package you need is not available via Conda.

If you end up in a position where you need to supplement your Conda environment with pip modules, please read [this blog post](https://www.anaconda.com/blog/using-pip-in-a-conda-environment).

To install this repo's conda environment, first install [Conda](https://docs.conda.io/projects/conda/en/stable/user-guide/install/index.html) then run:
```shell
conda create env -f environment.yml
```

To install this repo's pip environment, **first** [setup and activate an appropriately named virtual environment](https://python.land/virtual-environments/virtualenv) specifically for your project then run:
```shell
pip install -r requirements.txt
```

## Running the script

The script uses [Click](https://click.palletsprojects.com/en/8.1.x/) to handle command line arguments, it's simple to use and you end up nice user experience with minimal effort. Python's inbuilt [`argparse`](https://docs.python.org/3/library/argparse.html) tool is also a good alternative tool for this. 

After setting up the environment (see above), you can run this script via:

```shell
python does_it_flood.py --input_file_path tests/test_data/input_data.txt --output_file_path tests/test_data/output/output.txt --lat 1 --lon 2
```

## Pre-commit

Pre-commit hooks are tools that check or alter your code changes before allowing you to commit them. They not only prevent bugs from entering the code base but also help enforce code quality/style standards. Essentially, they automate parts of the code review process than can reasonably be automated.

One thing to note is that if the pre-commit hooks fail, the commit does not happen. You need to `git add` the changes that "hooks" request or have automatically applied and then try committing again.

To install pre-commit hooks for this repo (or any repo with a `.pre-commit-config.yaml`): 
```shell
pre-commit install
```

## GitHub Actions

The GitHub Action (GHA) defined in [`ci.yaml`](.github/workflows/ci.yaml) will run when a pull request is created or altered **and** when any code is pushed or merged to the `main` branch of a repo.


## Tests

It is highly recommended to build at least a simple "end to end" test for any project (see the `tests/` directory). Ideally this will run via Github Actions whenever you create or alter a pull request or merge anything into your `main` branch.

The purpose of tests is not only to tell you when you've broken something but also to document what your code is supposed to do. They take effort to build initially but they are worth the investment.

To run the tests in this repo (from the root directory of this project):

```shell
pytest
```

## Useful resources

- [Naming conventions](https://visualgit.readthedocs.io/en/latest/pages/naming_convention.html)