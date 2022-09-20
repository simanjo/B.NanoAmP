import pytest
from controller import get_conda_setup, check_pkgs, set_conda_envs
from controller import get_conda_version


def setup_conda():
    if get_conda_version() is None:
        return False
    envs, prefs = get_conda_setup()
    status, _ = check_pkgs(envs)
    if status == "complete":
        set_conda_envs(envs, prefs)
        return True
    else:
        return False


def pytest_collection_modifyitems(items):
    if not setup_conda():
        skip = pytest.mark.skip(
            reason="The necessary conda setup could not be found."
        )
        for item in items:
            if "needs_conda" in item.keywords:
                item.add_marker(skip)
