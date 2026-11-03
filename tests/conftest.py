from pathlib import Path

import pytest

# Two rows share a location, so these three rows cover two unique tiles.
VALID_CSV = "Latitude,Longitude\n23.03,72.58\n12.9789,77.5917\n23.03,72.58\n"


@pytest.fixture
def write_csv(tmp_path: Path):
    """Return a helper that writes ``content`` to a CSV file in ``tmp_path``."""

    def _write(content: str, name: str = "coords.csv") -> Path:
        path = tmp_path / name
        path.write_text(content)
        return path

    return _write


@pytest.fixture
def valid_csv(write_csv) -> Path:
    return write_csv(VALID_CSV)
