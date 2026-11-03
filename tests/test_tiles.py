import pytest

from geometry_logic.tiles import tile_name_from_point

# Expected values were generated with fathom_pyutils.tiles.TileGeom.from_point
# (v1.6.1) so this module stays identical to Fathom's tile scheme.


@pytest.mark.parametrize(
    ("lon", "lat", "expected"),
    [
        (72.58, 23.03, "n23e072"),
        (77.5917, 12.9789, "n12e077"),
        (-43.2, -22.9, "s23w044"),
        (-0.5, -0.5, "s01w001"),
        # The north-west offset: whole-degree lines belong to the tile to the
        # south and east.
        (0, 0, "s01e000"),
        (72, 23, "n22e072"),
        (72.0, 24.0, "n23e072"),
        (0.0001, 0.0001, "s01e000"),
        (-0.0001, 0.0002, "n00e000"),
        # Antimeridian and poles.
        (-180, 0, "s01w180"),
        (-179.5, 0, "s01w180"),
        (179.9999, 0, "s01e180"),
        (180, 0, "s01e180"),
        (180, 90, "n89e180"),
        (-180, -89.5, "s90w180"),
    ],
)
def test_tile_name_from_point(lon, lat, expected):
    assert tile_name_from_point(lon, lat) == expected


@pytest.mark.parametrize(
    ("lon", "lat", "match"),
    [
        (-180.1, 0, "Longitude"),
        (180.1, 0, "Longitude"),
        (0, 90.1, "Latitude must be"),
        (0, -90.1, "Latitude must be"),
        (0, -90, "southernmost tile"),
    ],
)
def test_tile_name_from_point_rejects_invalid(lon, lat, match):
    with pytest.raises(ValueError, match=match):
        tile_name_from_point(lon, lat)
