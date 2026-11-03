# num-tiles

A small CLI that counts how many unique Fathom tiles a set of coordinates falls into.

Give it a CSV with `Latitude` and `Longitude` columns and it reports the number of
distinct tiles (as defined by `fathom_pyutils.tiles.TileGeom`) that cover those
points.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/):

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

- Access to the private `fathom-pyutils` registry on GCP:
  1. Install `gcloud` and run `gcloud auth application-default login`. Your email
     must already have access to the registry project.
  2. Set up keyring auth for uv (see
     [fathom-pyutils](https://github.com/fathom-global/fathom-pyutils)):

     ```bash
     export UV_KEYRING_PROVIDER=subprocess
     export UV_INDEX_PRIVATE_REGISTRY_USERNAME=oauth2accesstoken
     ```

## Setup

```bash
uv sync
uv run pre-commit install --hook-type pre-commit --hook-type pre-push
```

## Usage

```bash
uv run num-tiles --filepath path/to/coordinates.csv
uv run num-tiles -f path/to/coordinates.csv --verbose   # log the tile for every row
```

The input CSV must contain `Latitude` and `Longitude` columns. Other columns are
ignored, and whitespace around header names is stripped:

```csv
Latitude,Longitude
23.03,72.58
12.9789,77.5917
```

Exit codes:

| Code | Meaning                                   |
| ---- | ----------------------------------------- |
| 0    | Success                                   |
| 1    | Invalid CSV (missing columns, empty file) |
| 2    | Usage error (e.g. file does not exist)    |

## Development

| Task                 | Command                                |
| -------------------- | -------------------------------------- |
| Run tests + coverage | `uv run pytest`                        |
| Lint                 | `uv run ruff check .`                  |
| Format               | `uv run ruff format .`                 |
| Type-check           | `uv run ty check`                      |
| All hooks            | `uv run pre-commit run --all-files`    |
| Add a dependency     | `uv add <package>`                     |
| Add a dev dependency | `uv add --dev <package>`               |
| Upgrade the lockfile | `uv lock --upgrade`                    |
| Update hook versions | `uv run pre-commit autoupdate`         |

Commit `pyproject.toml` and `uv.lock` together. Pre-commit runs ruff, ty and
`uv lock` on every commit, and the test suite on every push.

CI (`.github/workflows/ci.yaml`) has two jobs. `lint` runs ruff. `test` signs in to
GCP with Workload Identity Federation, installs dependencies from `uv.lock`, then
runs ty and pytest. It uses the `WORKLOAD_IDENTITY_PROVIDER` and
`SERVICE_ACCOUNT_EMAIL` Actions secrets.

Dependabot (`.github/dependabot.yml`) opens weekly PRs. Minor and patch updates
are grouped into one PR for runtime dependencies, one for dev dependencies and
one for GitHub Actions. Each major update gets its own PR. Pre-commit hook
versions aren't covered here; update them with `uv run pre-commit autoupdate`.

## Project layout

```text
src/geometry_logic/
  num_tiles.py      # CLI entry point and tile-counting logic
tests/
  conftest.py       # shared fixtures
  test_num_tiles.py
pyproject.toml      # project metadata, dependencies and tool config
uv.lock             # pinned dependency versions
```
