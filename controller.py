import os

import conda.cli.python_api as conda_api
import dearpygui.dearpygui as dpg
from packaging import version

import model


def get_conda_setup():
    prefs = {}
    envs = {}
    for env_name, pref in _get_conda_envs():
        if env_name == "":
            continue
        pkgs_in_env = []
        for pkg_name, ver in _get_conda_packages(env_name):
            if pkg_name in model.BINARIES:
                pkgs_in_env.append(pkg_name)
                if (not (pkg_name in prefs.keys() and
                        version.parse(prefs[pkg_name][1]) > version.parse(ver))
                    or env_name.startswith("nanoamp_")):
                    prefs[pkg_name] = (pref, ver)
        if pkgs_in_env:
            envs[env_name] = pkgs_in_env
    return envs, prefs

def _get_conda_envs():
    stdout, stderr, ret = conda_api.run_command(
        conda_api.Commands.INFO, "--envs"
    )
    if ret != 0:
        raise OSError(ret, stderr)
    return [
        (
            env.split()[0] if len(env.split()) > 1 else "",
            env.split()[-1]
        )
        for env in stdout.splitlines()[2:-1]
    ]

def _get_conda_packages(env):
    stdout, stderr, ret = conda_api.run_command(
        conda_api.Commands.LIST,
        ["-n", env]
    )
    if ret != 0:
        raise OSError(ret, stderr)
    return [(i.split()[0], i.split()[1]) for i in stdout.splitlines()[3:]]

def set_conda_envs(envs, prefs):
    pass

def init_conda_envs():
    for name, yml in model.get_conda_ymls():
        stdout, stderr, ret = conda_api.run_command(
                conda_api.Commands.CREATE, "-n", name
            )
        stdout, stderr, ret = conda_api.run_command(
                conda_api.Commands.INSTALL, "-n", name, "--file", yml,
                "--channel", "bioconda", "--channel", "conda-forge"
            )
        if ret != 0:
            raise OSError(ret, stderr)
        print(stdout)

def execute_pipeline():
    _setup_pipeline()
    _run_pipeline()
    print("Executing")

def _setup_pipeline():
    pass

def _run_pipeline():
    pass