import click
from pathlib import Path


@click.command()
@click.option(
    "--input_file_path", help="File path to input file.", type=click.Path(exists=True)
)
@click.option(
    "--output_file_path",
    help="File path to output file.",
    type=click.Path(exists=False),
)
@click.option(
    "--lat", prompt="Latitude", help="Latitude of your asset.", type=click.FLOAT
)
@click.option(
    "--lon", prompt="Longitude", help="Longitude of your asset.", type=click.FLOAT
)
def does_it_flood(
    input_file_path: str,  # the ": str" here is a type hint
    output_file_path: str,  # it tells developers that this argument should be a string
    lat: float,  # code editors like PyCharm also use this, they are highly recommended!
    lon: float,
) -> None:
    """
    Given a latitude and longitude, this command reads very complex data from input_file_path and
    writes a very useful answer to a file at output_file_path
    """
    output_file = Path(output_file_path)
    # this will create intermediate directories that don't exist
    output_file.parent.mkdir(exist_ok=True, parents=True)
    click.echo(
        f'Calculating the answer to the question "Will my asset at ({lat},{lon}) flood?"'
    )
    with open(input_file_path, "r", encoding="utf8") as input_file:
        answer = input_file.readline()
    output_file.write_text(f"The answer is {answer}", encoding="utf8")
    click.echo("Finished!")


if __name__ == "__main__":
    does_it_flood()  # pylint: disable=no-value-for-parameter
