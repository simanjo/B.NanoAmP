import pytest

from controller import _get_closest_guppy_ver, get_closest_model


@pytest.mark.parametrize(
    "guppy_version, versions, expected",
    [
        # last in list
        (
            "Guppy 3.5.1",
            ["Guppy 3.0.3", "Guppy 3.2.2", "Guppy 3.5.1"],
            "Guppy 3.5.1"
        ),
        # first in list
        (
            "Guppy 3.5.1",
            ["Guppy 3.5.1", "Guppy 4.0.11", "Guppy 5.0.7", "Guppy 5.0.15"],
            "Guppy 3.5.1"
        ),
        # in list
        (
            "Guppy 5.0.7",
            ["Guppy 3.6.0", "Guppy 4.0.11", "Guppy 5.0.7", "Guppy 5.0.15"],
            "Guppy 5.0.7"
        ),
        # smaller than all in list
        (
            "Guppy 3.0.6",
            ["Guppy 3.6.0", "Guppy 4.0.11", "Guppy 5.0.7", "Guppy 5.0.15"],
            "Guppy 3.6.0"
        ),
        # larger than all in list
        (
            "Guppy 6.0.10",
            ["Guppy 3.6.0", "Guppy 4.0.11", "Guppy 5.0.7", "Guppy 5.0.15"],
            "Guppy 5.0.15"
        ),
        # not in list
        (
            "Guppy 5.0.7",
            ["Guppy 3.6.0", "Guppy 4.0.11", "Guppy 5.0.15", "Guppy 5.1.4"],
            "Guppy 4.0.11"
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
