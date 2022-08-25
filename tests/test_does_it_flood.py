import filecmp
import os

from click.testing import CliRunner
from useful_scripts.does_it_flood import does_it_flood


def test_does_it_flood():
    """
    This is simple a way to test your script:
    1: Build a small input dataset with a corresponding expected output dataset.
    2: Set up a test that runs the script using the input data
    3: Compare the result from that run with the expected output dataset.
    """
    runner = CliRunner()
    output_file_path = "tests/test_data/output/test_output_data.txt"
    # remove output data from previous runs of this test
    _ = os.remove(output_file_path) if os.path.exists(output_file_path) else None

    # run the script using test input data
    runner.invoke(
        does_it_flood,
        [
            "--input_file_path",
            "tests/test_data/input_data.txt",
            "--output_file_path",
            output_file_path,
            "--lat",
            1,
            "--lon",
            2,
        ],
    )

    # compare the data that was written from this run with the expected output
    assert filecmp.cmp("tests/test_data/expected_output_data.txt", output_file_path)
