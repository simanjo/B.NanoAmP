import os
import subprocess

import dearpygui.dearpygui as dpg
from packaging import version

from PipelineSteps import DuplexStep, FilterStep, AssemblyStep
from PipelineSteps import RaconPolishingStep, MedakaPolishingStep
from PipelineSteps import CleanDuplexStep, CleanFilterStep
from PipelineSteps import CleanAssemblyStep, FinalCleanStep
from ErrorWindow import ErrorWindow
import model


def execute_pipeline():
    if not _preflight_check():
        return
    steps, folder_iter = _setup_pipeline()
    for folder in folder_iter:
        print(f"Executing in {folder}")
        for step in steps:
            step.run(folder)


def _preflight_check():
    bcfolder = dpg.get_value("bcfolder")
    if not os.path.isdir(bcfolder):
        msg = "Please specify a valid folder location."
        msg += f"\nThe given folder '{bcfolder}' does not exist."
        ErrorWindow(msg)
        return False
    return True


def _use_folder(path):
    if not path.is_dir() or not _has_fastq(path):
        return False
    # safeguard against rerun from unclean environment
    # we do not want to recurse deeper in original folder
    if path.name.startswith("original"):
        return False
    if dpg.get_value("skip_unclassified"):
        return not path.name.startswith("unclassified")
    return True


def _is_fastq(name):
    return (name.endswith(".fastq")
            or name.endswith(".fastq.gz"))


def _has_fastq(path):
    for entry in os.scandir(path):
        if (entry.is_file() and _is_fastq(entry.name)):
            return True


def _setup_pipeline():
    bcfolder = dpg.get_value("bcfolder")
    print(f"Working on files in {bcfolder}")

    def folder_iter():
        yield from (entry.path for entry in os.scandir(bcfolder)
                    if _use_folder(entry))
        yield from (_ for _ in [bcfolder] if _has_fastq(_))

    threads = dpg.get_value("threads")
    keep_intermediate = dpg.get_value("keep_intermediate")
    genome_size = dpg.get_value("genome_size")
    coverage = dpg.get_value("coverage")
    min_len = dpg.get_value("filtlong_minlen")
    bases = int(genome_size * 1_000_000 * coverage)
    medaka_mod = dpg.get_value("medaka_manumodel")
    is_racon = not dpg.get_value("racon_skip")

    steps = []
    steps.append(DuplexStep(threads))
    steps.append(FilterStep(min_len, bases))
    if not keep_intermediate:
        steps.append(CleanDuplexStep())

    for assembler in model.get_assemblers():
        if dpg.get_value(f"use_{assembler}"):
            steps.append(AssemblyStep(threads, assembler))

            if assembler == "Flye" and is_racon:
                steps.append(RaconPolishingStep(threads))

            steps.append(
                MedakaPolishingStep(threads, assembler, medaka_mod, is_racon)
            )
            if not keep_intermediate:
                steps.append(CleanAssemblyStep(assembler, is_racon))
    if not keep_intermediate:
        steps.append(CleanFilterStep())
        steps.append(FinalCleanStep())
    return steps, folder_iter()


#################### Conda Setup ####################

def get_conda_version():
    home_path = os.environ['HOME']
    conda_default_paths = [
        model.get_prefix("conda"),
        os.environ["PATH"],
        os.path.join(home_path, "miniconda3", "bin"),
        os.path.join(home_path, "anaconda3", "bin")
    ]
    env = {'PATH': ":".join(conda_default_paths)}
    try:
        conda_version = subprocess.run(
            ["conda", "--version"], capture_output=True, env=env
        ).stdout.split()[-1]
        conda_bin = subprocess.run(
            ["which", "conda"], capture_output=True, env=env
        ).stdout.decode()
        model.PREFIXES['conda'] = os.path.abspath(
            os.path.join(conda_bin, os.pardir)
        )
        return conda_version
    except FileNotFoundError:
        return None


def set_conda_envs(envs, prefs):
    prefixes = {
        pkg: os.path.join(pref, "bin") for pkg, (pref, _) in prefs.items()
    } | {'conda': model.get_prefix('conda')}
    model.PREFIXES = prefixes


def init_conda_envs():
    conda_path = model.get_prefix("conda")
    assert conda_path != ""
    print("doing conda init")

    for name, yml in model.get_conda_ymls():
        dpg.set_value("log_text", f"Checking for an environment named {name}")
        if name not in [i[0] for i in _get_conda_envs()]:
            print(f"Creating environment {name}")
            proc = subprocess.run(
                ["conda", "create", "-n", name, "--yes"], capture_output=True,
                env={'PATH': f"{os.environ['PATH']}:{conda_path}"}
            )
            if proc.returncode != 0:
                raise OSError(proc.returncode, proc.stderr.decode())
            print(proc.stdout.decode())
        dpg.set_value("log_text", f"Running install for {name}")
        proc = subprocess.run(
            [
                "conda", "install", "-n", name, "--file", yml,
                "--channel", "bioconda", "--channel", "conda-forge",
                "--channel", "default", "--yes"
            ], capture_output=True,
            env={'PATH': f"{os.environ['PATH']}:{conda_path}"}
        )
        if proc.returncode != 0:
            raise OSError(proc.returncode, proc.stderr.decode())
        print(proc.stdout.decode())
    set_conda_envs(*get_conda_setup())


def check_pkgs(envs):
    missing = []
    available = [pkg for pkgs in envs.values() for pkg in pkgs]
    for pkg_name in model.BINARIES:
        if pkg_name not in available:
            missing.append(pkg_name)
    status = "complete" if not missing else "incomplete"
    return status, missing


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
                if (
                    not (
                        pkg_name in prefs.keys()
                        and (version.parse(prefs[pkg_name][1])
                             > version.parse(ver))
                    ) or env_name.startswith("nanoamp_")
                ):
                    prefs[pkg_name] = (pref, ver)
        if pkgs_in_env:
            envs[env_name] = pkgs_in_env
    return envs, prefs


def _get_conda_envs():
    conda_path = model.get_prefix("conda")
    assert conda_path != ""

    proc = subprocess.run(
        ["conda", "info", "--envs"], capture_output=True,
        env={'PATH': f"{conda_path}:{os.environ['PATH']}"}
    )
    if proc.returncode != 0:
        raise OSError(proc.returncode, proc.stderr.decode())
    return [
        (
            env.split()[0] if len(env.split()) > 1 else "",
            env.split()[-1]
        )
        for env in proc.stdout.decode().splitlines()[2:-1]
    ]


def _get_conda_packages(env):
    conda_path = model.get_prefix("conda")
    assert conda_path != ""

    proc = subprocess.run(
        ["conda", "list", "-n", env], capture_output=True,
        env={'PATH': f"{os.environ['PATH']}:{conda_path}"}
    )
    if proc.returncode != 0:
        raise OSError(proc.returncode, proc.stderr)
    return [
        (i.split()[0], i.split()[1])
        for i in proc.stdout.decode().splitlines()[3:]
    ]

################## model selection


def filter_models(device=None, cell=None, guppy=None, variant=None):
    all_models = model.get_model_df()
    query = ""
    if device is not None:
        query += f"device == '{device}'"
    if cell is not None:
        if query != "":
            query += " & "
        query += f"cell == '{cell}'"
    if guppy is not None:
        if query != "":
            query += " & "
        query += f"guppy == '{guppy}'"
    if variant is not None:
        if query != "":
            query += " & "
        query += f"variant == '{variant}'"
    if query == "":
        return all_models
    return all_models.query(query)
