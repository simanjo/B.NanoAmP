from bisect import bisect_right
import os
import sys
import subprocess
import logging
from pathlib import Path
from glob import glob

from packaging import version
from dearpygui import dearpygui as dpg

from PipelineSteps import DuplexStep, FilterStep, AssemblyStep
from PipelineSteps import RaconPolishingStep, MedakaPolishingStep
from PipelineSteps import CleanDuplexStep, CleanFilterStep
from PipelineSteps import CleanAssemblyStep, FinalCleanStep
from ErrorWindow import ErrorWindow
from PipelineStepError import PipelineStepError
from CustomUILogHandler import CustomUILogHandler
import model


def execute_pipeline():
    dir = Path(dpg.get_value("bcfolder"))
    if not _preflight_check(dir):
        return
    _setup_logging(dir)
    dpg.configure_item("pipe_active_ind", show=True)
    logger = logging.getLogger("")
    logger.info(f"Finished preflight check using {dir} as working directory.")
    steps = _setup_pipeline()
    for folder in _fastq_folder_iter(dir):
        logger.info(f"  Executing choosen pipeline in {folder}")
        for step in steps:
            try:
                step.run(folder)
            except PipelineStepError:
                logger.error(f"Failed to execute pipeline in {folder}")
                logger.info("Attempting to perform cleanup...")
                # if step is clean step try to run it and fail silently
                for step in steps:
                    if isinstance(
                        step,
                        (
                            CleanDuplexStep, CleanFilterStep,
                            CleanAssemblyStep, FinalCleanStep
                        )
                    ):
                        try:
                            step.run(folder)
                        except Exception as e:
                            logger.exception(f"Failed cleanup in step {step}:")
                            logger.exception(e)
                            pass
                # final break, no use in continuing pipeline
                break
    dpg.configure_item("pipe_active_ind", show=False)


def _setup_logging(dir):
    if not (dir / "log").is_dir():
        os.mkdir(dir / "log")

    logging.basicConfig(
        format='%(asctime)s - %(levelname)s: %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler(filename=dir / "log" / "Pipeline.log"),
            CustomUILogHandler(
                "log_area", level=logging.INFO
            )
        ]
    )


def _preflight_check(dir):
    if not dir.is_dir():
        msg = "Please specify a valid folder location."
        msg += f"\nThe given folder '{dir}' does not exist."
        ErrorWindow(msg)
        return False
    # check for valid parameters
    for val, type in [
        ("threads", int),
        ("genome_size", float),
        ("coverage", int),
        ("filtlong_minlen", int),
        ("medaka_manumodel", str)
    ]:
        if not isinstance(dpg.get_value(val), type):
            dpg.add_text(
                f"The parameter {val} is incorrectly specified.",
                parent="log_area"
            )
            return False
    if dpg.get_value("medaka_manumodel") == "--":
        dpg.add_text(
            "Please specify a valid medaka model.", parent="log_area"
        )
        return False
    # check if folder contains fastq files
    # if generator is empty return False
    for _ in _fastq_folder_iter(dir):
        return True
    dpg.add_text(
        "Please specify a folder containing the fastq files.",
        parent="log_area"
    )
    return False


def _use_folder(folder):
    if not folder.is_dir() or not _has_fastq(folder):
        return False
    # safeguard against rerun from unclean environment
    # we do not want to recurse deeper in original folder
    if (
        folder.stem.startswith("original")
        or folder.stem.startswith("assemblies")
    ):
        return False
    if dpg.get_value("skip_unclassified"):
        return not folder.stem.startswith("unclassified")
    return True


def _has_fastq(folder):
    for entry in folder.iterdir():
        if (entry.is_file() and ".fastq" in entry.suffixes):
            return True


def _calculate_coverage(dir):
    # calculates coverage in the given directory via
    # awk '{if ((NR%=4)==2) print;}' *.fastq | wc | awk '{print $NF-$(NF-1);}'
    if sys.version_info < (3, 10):
        cwd = os.getcwd()
        os.chdir(dir)
        gzip_proc = subprocess.Popen(
            ["gunzip", "-fck"] + glob("*.fastq.gz"),
            cwd=dir, stdout=subprocess.PIPE
        )
        if not glob("*.fastq.gz"):
            awk_proc_1 = subprocess.Popen(
                ["awk", "{if ((NR%=4)==2) print;}"] + glob("*.fastq"),
                cwd=dir, stdout=subprocess.PIPE
            )
        else:
            awk_proc_1 = subprocess.Popen(
                ["awk", "{if ((NR%=4)==2) print;}", "-"] + glob("*.fastq"),
                cwd=dir, stdin=gzip_proc.stdout, stdout=subprocess.PIPE
            )
    else:
        gzip_proc = subprocess.Popen(
            ["gunzip", "-fck"] + glob("*.fastq.gz", root_dir=dir),
            cwd=dir, stdout=subprocess.PIPE
        )
        if not glob("*.fastq.gz"):
            awk_proc_1 = subprocess.Popen(
                ["awk", "{if ((NR%=4)==2) print;}"]
                + glob("*.fastq", root_dir=dir),
                cwd=dir, stdout=subprocess.PIPE
            )
        else:
            awk_proc_1 = subprocess.Popen(
                ["awk", "{if ((NR%=4)==2) print;}", "-"]
                + glob("*.fastq", root_dir=dir),
                cwd=dir, stdin=gzip_proc.stdout, stdout=subprocess.PIPE
            )
    wc_proc = subprocess.Popen(
        ["wc"], stdin=awk_proc_1.stdout, stdout=subprocess.PIPE
    )
    awk_proc_2 = subprocess.run(
        ["awk", "{print $NF-$(NF-1);}"], stdin=wc_proc.stdout,
        capture_output=True
    )
    bases = int(awk_proc_2.stdout.decode())
    genome_size = dpg.get_value("genome_size") * 1_000_000
    if sys.version_info < (3, 10):
        os.chdir(cwd)
    return bases / genome_size


def check_coverages(dir):
    if not dir.is_dir():
        ErrorWindow(f"The given path {dir} is not a directory.")
        return None
    msg = []
    for folder in _fastq_folder_iter(dir):
        cov = _calculate_coverage(folder)
        if folder == dir:
            folder_name = folder.relative_to(dir.parent)
        else:
            folder_name = folder.relative_to(dir)
        if cov < 30:
            msg.append(f"Coverage in {folder_name} is below 30x. ")
            msg.append("Typing might be imprecise and ",)
            msg.append("further sequencing is recommended.\n")
        elif cov < 50:
            msg.append(f"Coverage in {folder_name} is below 50x. ")
            msg.append("Racon polishing is recommended, ")
            msg.append("to potentially enhance typing.\n")
    if msg:
        ErrorWindow("".join(msg))


def _fastq_folder_iter(dir):
    yield from (
        entry for entry in dir.iterdir() if _use_folder(entry)
    )
    if _has_fastq(dir):
        yield dir


def _setup_pipeline():
    threads = dpg.get_value("threads")
    keep_intermediate = dpg.get_value("keep_intermediate")
    genome_size = dpg.get_value("genome_size")
    coverage = dpg.get_value("coverage")
    min_len = dpg.get_value("filtlong_minlen")
    bases = int(genome_size * 1_000) * 1_000 * coverage
    medaka_mod = dpg.get_value("medaka_manumodel")
    is_racon = not dpg.get_value("racon_skip")

    asms = [i for i in model.get_assemblers() if dpg.get_value(f"use_{i}")]
    logging.info("Setting up pipeline with the following parameters:")
    logging.info(f"  Threads: {threads}, Filtlong min-len: {min_len}")
    logging.info(f"  Genome Size: {genome_size}, Coverage: {coverage}")
    logging.info(f"  Assemblers: {asms}, Racon Polishing: {is_racon}")
    logging.info(f"  Medaka Model: {medaka_mod}")

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
    return steps


#################### Conda Setup ####################

def get_conda_version():
    conda_default_paths = [
        model.get_prefix("conda"),
        os.environ["PATH"],
        str(Path.home() / "miniconda3" / "bin"),
        str(Path.home() / "anaconda3" / "bin"),
        "/opt/anaconda3/bin",
        "/opt/miniconda3/bin"
    ]
    env = {'PATH': ":".join(conda_default_paths)}
    try:
        conda_version = subprocess.run(
            ["conda", "--version"], capture_output=True, env=env
        ).stdout.split()[-1]
        conda_bin = subprocess.run(
            ["which", "conda"], capture_output=True, env=env
        ).stdout.decode()
        model.PREFIXES['conda'] = Path(conda_bin).parent
        return conda_version
    except FileNotFoundError:
        return None


def set_conda_envs(envs, prefs):
    # TODO: {} | {} to merge dicts is only introduced in python 3.9
    # find better way to express
    prefixes = {
        **{
            pkg: pref + "/bin" for pkg, (pref, _) in prefs.items()
        },
        **{'conda': model.get_prefix('conda')}
    }
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


def _get_closest_guppy_ver(guppy_version, versions):
    def _get_ver_tuple(ver):
        # strip leading 'g' and then assume the next two chars
        # are major and minor version and the remainder is build number
        return (int(ver[1]), int(ver[2]), int(ver[3:]))
    vers = sorted(versions, key=_get_ver_tuple)
    pos = bisect_right(
        vers,
        _get_ver_tuple(guppy_version),
        key=_get_ver_tuple
    )
    return vers[max(0, pos - 1)]


def get_closest_model(cell, device, guppy, variant):
    all_models = model.get_model_df()

    # check for cell, device and variant
    query = f"cell == '{cell}' & device == '{device}' & variant == '{variant}'"
    filtered = all_models.query(query)
    if len(filtered.guppy) == 1:
        return filtered.full_model.iloc[0]
    if not filtered.empty:
        guppy_ver = _get_closest_guppy_ver(guppy, filtered.guppy)
        query += f" & guppy == '{guppy_ver}'"
        return all_models.query(query).full_model.iloc[0]

    # check for cell, variant and empty device
    if not device == "":
        query = f"cell == '{cell}' & variant == '{variant}' & device == ''"
        filtered = all_models.query(query)
        if len(filtered.guppy) == 1:
            return filtered.full_model.iloc[0]
        if not filtered.empty:
            guppy_ver = _get_closest_guppy_ver(guppy, filtered.guppy)
            query += f" & guppy == '{guppy_ver}'"
            return all_models.query(query).full_model.iloc[0]

    # check for cell and variant
    query = f"cell == '{cell}' & variant == '{variant}'"
    filtered = all_models.query(query)
    if len(filtered.guppy) == 1:
        return filtered.full_model.iloc[0]
    if not filtered.empty:
        guppy_ver = _get_closest_guppy_ver(guppy, filtered.guppy)
        query += f" & guppy == '{guppy_ver}'"
        return all_models.query(query).full_model.iloc[0]

    return None
