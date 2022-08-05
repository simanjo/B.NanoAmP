import os

import conda.cli.python_api as conda_api
import dearpygui.dearpygui as dpg
from packaging import version

from PipelineSteps import DuplexStep, FilterStep, AssemblyStep
from PipelineSteps import RaconPolishingStep, MedakaPolishingStep
import model


def execute_pipeline():
    steps, folder_iter = _setup_pipeline()
    for folder in folder_iter:
        print(f"Executing in {folder}")
        # for step in steps:
        #     step.run(folder.path)

def _use_folder(path):
    if not path.is_dir() or not _has_fastq(path):
        return False
    if dpg.get_value("skip_unclassified"):
        return not path.name.startswith("unclassified")
    return True

def _has_fastq(path):
    for entry in os.scandir(path):
        if (entry.is_file() and (
                entry.name.endswith(".fastq") or
                entry.name.endswith(".fastq.gz")
            )):
            return True

def _setup_pipeline():
    bcfolder = dpg.get_value("bcfolder")
    print(f"Working on files in {bcfolder}")


    def folder_iter():
        yield from (_ for _ in [bcfolder] if _has_fastq(_))
        yield from (entry.path for entry in os.scandir(bcfolder)
                    if _use_folder(entry))

    threads = dpg.get_value("threads")
    steps = []
    steps.append(DuplexStep(threads))

    min_len = dpg.get_value("filtlong_minlen")
    bases = dpg.get_value("filtlong_bases")
    steps.append(FilterStep(min_len, bases))

    assembler = dpg.get_value("assembler_sel")
    steps.append(AssemblyStep(threads, assembler))

    is_racon = not dpg.get_value("racon_skip")
    if assembler == "Flye" and is_racon:
        steps.append(RaconPolishingStep(threads))

    model = dpg.get_value("medaka_manumodel")
    steps.append(MedakaPolishingStep(threads, assembler, model, is_racon))
    return steps, folder_iter()


#################### Conda Setup ####################

def set_conda_envs(envs, prefs):
    prefixes = {pkg: os.path.join(pref, "bin") for pkg,(pref,_) in prefs.items()}
    model.PREFIXES = prefixes

def init_conda_envs():
    for name, yml in model.get_conda_ymls():
        stdout, stderr, ret = conda_api.run_command(
                conda_api.Commands.CREATE, "-n", name
            )
        if ret != 0:
            raise OSError(ret, stderr)
        print(stdout)
        stdout, stderr, ret = conda_api.run_command(
                conda_api.Commands.INSTALL, "-n", name, "--file", yml,
                "--channel", "bioconda", "--channel", "conda-forge", "--yes"
            )
        if ret != 0:
            raise OSError(ret, stderr)
        print(stdout)

def check_pkgs(envs):
    missing = []
    available = [pkg for pkgs in envs.values() for pkg in pkgs]
    for pkg_name in model.BINARIES:
        if pkg_name not in available:
            missing.append(pkg_name)
    status = "complete" if not missing else "incomplete"
    return status, missing

def check_guppy():
    pass

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
