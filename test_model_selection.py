import pytest

from controller import _get_closest_guppy_ver, get_closest_model


@pytest.mark.parametrize(
    "guppy_version, versions, expected",
    [
        # last in list
        (
            "g351",
            ["g303", "g322", "g351"],
            "g351"
        ),
        # first in list
        (
            "g351",
            ["g351", "g4011", "g507", "g5015"],
            "g351"
        ),
        # in list
        (
            "g507",
            ["g360", "g4011", "g507", "g5015"],
            "g507"
        ),
        # smaller than all in list
        (
            "g306",
            ["g360", "g4011", "g507", "g5015"],
            "g360"
        ),
        # larger than all in list
        (
            "g6010",
            ["g360", "g4011", "g507", "g5015"],
            "g5015"
        ),
        # not in list
        (
            "g507",
            ["g360", "g4011", "g5015", "g514"],
            "g4011"
        )
    ]
)
def test_get_closest_guppy_version(guppy_version, versions, expected):
    assert expected == _get_closest_guppy_ver(guppy_version, versions)


@pytest.mark.parametrize(
    "params, expected",
    [
        # same model as requested
        (
            ("r103", "min", "g360", "high"), "r103_min_high_g360"
        ),
        # smaller guppy version requested
        (
            ("r103", "prom", "g303", "high"), "r103_prom_high_g360"
        ),
        # smaller guppy version requested
        (
            ("r103", "prom", "g303", "high"), "r103_prom_high_g360"
        ),
        # no device, smaller guppy version
        (
            ("r103", "", "g303", "high"), "r103_min_high_g345"
        ),
        # no variant found
        (
            ("r10", "min", "g360", "hac"), None
        )
    ]
)
@pytest.mark.needs_conda
def test_get_closest_model(params, expected):
    cell, device, guppy, variant = params
    assert expected == get_closest_model(cell, device, guppy, variant)
