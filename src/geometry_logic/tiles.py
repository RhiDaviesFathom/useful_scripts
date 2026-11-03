"""Map coordinates to Fathom's 1-degree tile names.

Tile names use the latitude and longitude of the tile's lower-left corner, for
example ``n23e072`` or ``s01w180``.

Fathom tiles are shifted north-west by half a 3-arcsecond pixel (1/7200 degree).
A point on an exact whole-degree line therefore belongs to the tile to its south
and east of it: (lon 0, lat 0) is in ``s01e000``, not ``n00e000``.
"""

import math

# North-west shift applied to tile boundaries, in degrees. Rounded to 8 decimal
# places to match the boundaries used by Fathom's flood-map tiles.
OFFSET_X = round(-1 / 7200, 8)
OFFSET_Y = round(1 / 7200, 8)


def tile_name_from_point(lon: float, lat: float) -> str:
    """Return the name of the 1-degree tile that contains a point.

    Args:
        lon: Longitude in degrees, from -180 to 180.
        lat: Latitude in degrees, from -90 to 90.

    Returns:
        The tile name, e.g. ``"n23e072"``.

    Raises:
        ValueError: If the point is outside the valid range, or falls south of
            the southernmost tile (latitude within 1/7200 degree of -90).
    """
    if not -180 <= lon <= 180:
        msg = f"Longitude must be between -180 and 180: {lon}"
        raise ValueError(msg)
    if not -90 <= lat <= 90:
        msg = f"Latitude must be between -90 and 90: {lat}"
        raise ValueError(msg)

    # The range checks above keep tile_lon within [-180, 180], so it never has
    # to wrap around the antimeridian. Only the south pole needs a check.
    tile_lat = math.floor(lat - OFFSET_Y)
    tile_lon = math.floor(lon - OFFSET_X)
    if tile_lat < -90:
        msg = f"Latitude {lat} is south of the southernmost tile."
        raise ValueError(msg)

    lat_prefix = "s" if tile_lat < 0 else "n"
    lon_prefix = "w" if tile_lon < 0 else "e"
    return f"{lat_prefix}{abs(tile_lat):02d}{lon_prefix}{abs(tile_lon):03d}"
