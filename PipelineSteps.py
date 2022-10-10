import gzip
import logging
from pathlib import Path
import sys
import os
import shutil
import subprocess
from abc import ABC, abstractmethod
import time
from PipelineStepError import PipelineStepError

import model

def _check_and_log_output(
    logger, proc, log_name, current_call, step, poll=False
):
    if poll:
        # HACK: call proc.poll to get returncode
        while proc.poll() is None:
            time.sleep(1)
    # HACK: get log dir by guessing that first encountered file handler
    # contains the logfile (and always exists...) and dump output to it
    log_dir = None
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler):
            log_dir = Path(h.baseFilename).parent
            break
    if proc.returncode == 0:
        with open(log_dir / log_name, "a") as fh:
            if not poll and proc.stdout is not None:
                fh.write(proc.stdout.decode())
                fh.write("\n")
            if proc.stderr is not None:
                fh.write(proc.stderr.decode())
                fh.write("\n")
    else:
        with open(log_dir / log_name, "a") as fh:
            if proc.stderr is not None:
                fh.write(proc.stderr.decode())
        logger.error("Failed to execute current step given by")
        logger.error(f"{current_call}")
        raise PipelineStepError(step)

class PipelineStep(ABC):

    @abstractmethod
    def run(self, wdir: str) -> None:
        pass


class DuplexStep(PipelineStep):

    def __init__(self, threads) -> None:
        self.threads = threads

    def run(self, wdir):
        logger = logging.getLogger("")
        logger.info("Started Duplex step.")

        duplex_call, env = _get_duplex_call(self.threads, wdir)
        proc = subprocess.run(
            duplex_call, capture_output=True, env=env
        )
        _check_and_log_output(
            logger, proc, "DuplexStep.log", duplex_call, self
        )

        orig_dir = wdir / "original"
        orig_dir.mkdir()
        for entry in wdir.iterdir():
            if entry.is_file() and ".fastq" in entry.suffixes:
                # shutil needs str-like src directory until python 3.9
                # https://bugs.python.org/issue32689
                if sys.version_info < (3, 9):
                    shutil.move(str(entry), orig_dir)
                else:
                    shutil.move(entry, orig_dir)
        logger.info("  Moved original files to subfolder")

        # copy everything
        outfile = wdir / f"{wdir.stem}.fastq.gz"
        split_dir = wdir / f"{wdir.stem}_split"
        with gzip.open(outfile, 'wb') as out_fh:
            for entry in split_dir.iterdir():
                if entry.is_file():
                    if entry.suffix == ".gz":
                        with gzip.open(entry, 'rb') as in_fh:
                            shutil.copyfileobj(in_fh, out_fh)
                    elif entry.suffix == ".fastq":
                        with open(entry, 'rb') as in_fh:
                            shutil.copyfileobj(in_fh, out_fh)
        logger.info("  and sucessfully concatenated them.")
        logger.info("Finished Duplex step.")


class CleanDuplexStep(PipelineStep):
    def __init__(self) -> None:
        pass

    def run(self, wdir):
        shutil.rmtree(wdir / f"{wdir.stem}_split")
        logging.info("Cleaned up Duplex step.")


class FilterStep(PipelineStep):

    def __init__(self, min_len, target_bases) -> None:
        self.min_len = min_len
        self.target_bases = target_bases

    def run(self, wdir: str) -> None:
        logger = logging.getLogger("")
        logger.info("Started Filtlong step.")

        filtered_dir = wdir / "filtered_reads"
        filtered_dir.mkdir()

        filtlong_call, env = _get_filtlong_call(
            self.min_len, self.target_bases, wdir
        )
        proc = subprocess.Popen(
            filtlong_call, stdout=subprocess.PIPE, env=env
        )
        filtered_reads = filtered_dir / f"{wdir.stem}_filtered.fastq.gz"

        with gzip.open(filtered_reads, 'wb') as out_fh:
            shutil.copyfileobj(proc.stdout, out_fh)
        # TODO: output gets dumped to console, how to capture that?
        _check_and_log_output(
            logger, proc, "FiltlongStep.log", filtlong_call, self, poll=True
        )


class CleanFilterStep(PipelineStep):
    def __init__(self) -> None:
        pass

    def run(self, wdir):
        shutil.rmtree(wdir / "filtered_reads")
        logging.info("Cleaned up Filtlong step.")


class AssemblyStep(PipelineStep):

    def __init__(self, threads, assembler) -> None:
        self.threads = threads
        self.assembler = assembler

    def run(self, wdir: str) -> None:
        logger = logging.getLogger("")
        logger.info(f"Started Assembly step using {self.assembler}")
        if self.assembler == "Miniasm":
            asm_dir = wdir / f"{wdir.stem}_miniasm_assembly"
            asm_dir.mkdir()

            minimap_call, env = _get_minimap_overlap(
                self.threads, wdir
            )
            proc = subprocess.Popen(
                minimap_call, stdout=subprocess.PIPE, env=env
            )
            read_overlap = asm_dir / f"{wdir.stem}_overlap.paf.gz"
            with gzip.open(read_overlap, 'wb') as out_fh:
                shutil.copyfileobj(proc.stdout, out_fh)
            _check_and_log_output(
                logger, proc, "MiniasmStep.log", minimap_call, self, poll=True
            )
            logger.info("  Finished minimap execution.")

            assembler_call, env = _get_miniasm_call(
                self.threads, wdir
            )
            proc = subprocess.Popen(
                assembler_call, stdout=subprocess.PIPE, env=env
            )
            asm_output = asm_dir / f"{wdir.stem}_unpolished_assembly.gfa"
            with open(asm_output, 'wb') as out_fh:
                shutil.copyfileobj(proc.stdout, out_fh)
            _check_and_log_output(
                logger, proc, "MiniasmStep.log", assembler_call, self, True
            )
            logger.info("  Finished miniasm execution.")

            polish_call, env = _get_minipolish_call(
                self.threads, wdir
            )
            proc = subprocess.Popen(
                polish_call, stdout=subprocess.PIPE, env=env
            )
            polish_output = asm_dir / f"{wdir.stem}_assembly.gfa"
            with open(polish_output, 'wb') as out_fh:
                shutil.copyfileobj(proc.stdout, out_fh)
            _check_and_log_output(
                logger, proc, "MiniasmStep.log", polish_call, self, poll=True
            )
            logger.info("  Finished minipolish execution.")

            ################# HACK
            # cf awk '/^S/{print ">"$2"\n"$3}' assmb.gfa | fold > assmb.fasta
            fasta_conv_awk = subprocess.Popen(
                ["awk", "/^S/{print \">\"$2\"\\n\"$3}", polish_output],
                stdout=subprocess.PIPE
            )
            fasta_conv_fold = subprocess.Popen(
                ["fold"], stdin=fasta_conv_awk.stdout, stdout=subprocess.PIPE
            )
            asm_output = asm_dir / "assembly.fasta"
            with open(asm_output, 'wb') as out_fh:
                shutil.copyfileobj(fasta_conv_fold.stdout, out_fh)
            ######################
            logger.info("  Finished conversion to fasta format.")

        elif self.assembler == "Flye":
            assembler_call, env = _get_flye_call(
                self.threads, wdir
            )
            # print(f"running {assembler_call}")
            proc = subprocess.run(
                assembler_call, capture_output=True, env=env
            )
            _check_and_log_output(
                logger, proc, "FlyeStep.log", assembler_call, self
            )
            logging.info("  Finished Flye execution.")

        elif self.assembler == "Raven":
            asm_dir = wdir / f"{wdir.stem}_raven_assembly"
            asm_dir.mkdir()
            assembler_call, env = _get_raven_call(
                self.threads, wdir
            )
            proc = subprocess.Popen(
                assembler_call, stdout=subprocess.PIPE, env=env
            )
            asm_output = asm_dir / "assembly.fasta"
            with open(asm_output, 'wb') as out_fh:
                shutil.copyfileobj(proc.stdout, out_fh)
            _check_and_log_output(
                logger, proc, "RavenStep.log", assembler_call, self, poll=True
            )
            logging.info("  Finished Raven execution.")

        else:
            raise NotImplementedError(
                f"The assembly method {self.assembler} is not supported."
            )


class CleanAssemblyStep(PipelineStep):
    def __init__(self, assembler, is_racon) -> None:
        self.assembler = assembler
        self.is_racon = is_racon and assembler == "Flye"

    def run(self, wdir):
        asm_name = f"{wdir.stem}_{self.assembler.lower()}_assembly"
        shutil.rmtree(wdir / asm_name)

        if self.is_racon:
            mapping_dir = wdir / "nanopore_mapping"
            polish_dir = wdir / f"{wdir.stem}_racon_polishing"
            shutil.rmtree(mapping_dir)
            shutil.rmtree(polish_dir)

        medaka_dir = wdir / "medaka_polished"
        shutil.rmtree(medaka_dir)

        asm_dir = wdir / "assemblies"
        if not asm_dir.is_dir():
            asm_dir.mkdir()
        polish_flag = "rm" if self.is_racon else "m"
        fasta = f"{wdir.stem}_{self.assembler}_{polish_flag}_coverage.fasta"
        if (wdir / fasta).is_file():
            # shutil needs str-like src directory until python 3.9
            # https://bugs.python.org/issue32689
            if sys.version_info < (3, 9):
                shutil.move(str(wdir / fasta), asm_dir)
            else:
                shutil.move(wdir / fasta, asm_dir)
        logging.info(f"  Finished cleanup for {self.assembler} step.")


class RaconPolishingStep(PipelineStep):
    def __init__(self, threads) -> None:
        self.threads = threads

    def run(self, wdir):
        logger = logging.getLogger("")
        logger.info("Started racon polishing.")
        mapping_dir = wdir / "nanopore_mapping"
        mapping_dir.mkdir()
        polish_dir = wdir / f"{wdir.stem}_racon_polishing"
        polish_dir.mkdir()

        minimap_call, env = _get_minimap_mapping(
            self.threads, wdir
        )
        proc = subprocess.Popen(
            minimap_call, stdout=subprocess.PIPE, env=env
        )
        mapping = mapping_dir / "mapping.sam"
        with open(mapping, 'wb') as out_fh:
            shutil.copyfileobj(proc.stdout, out_fh)
        _check_and_log_output(
            logger, proc, "RaconStep.log", minimap_call, self, poll=True
        )
        logging.info("  Finished minimap execution.")

        polishing_call, env = _get_racon_call(
            self.threads, wdir
        )
        proc = subprocess.Popen(
            polishing_call, stdout=subprocess.PIPE, env=env
        )
        polish_out = polish_dir / "assembly.fasta"
        with open(polish_out, 'wb') as out_fh:
            shutil.copyfileobj(proc.stdout, out_fh)
        _check_and_log_output(
            logger, proc, "RaconStep.log", polishing_call, self, poll=True
        )
        logging.info("  Finished racon polishing.")


class MedakaPolishingStep(PipelineStep):
    def __init__(self, threads, assembler, model, is_racon) -> None:
        self.threads = threads
        self.assembler = assembler
        self.model = model
        self.is_racon = is_racon and assembler == "Flye"
        super().__init__()

    def run(self, wdir):
        logger = logging.getLogger("")
        logger.info("Started medaka polishing.")
        medaka_call, env = _get_medaka_call(
            self.threads, self.assembler,
            self.model, self.is_racon, wdir
        )
        proc = subprocess.run(
            medaka_call, capture_output=True, env=env
        )
        _check_and_log_output(
            logger, proc, "MedakaStep.log", medaka_call, self
        )
        logger.info("  Finished medaka polishing.")
        polish = "rm" if self.is_racon else "m"
        fasta = f"{wdir.stem}_{self.assembler}_{polish}_coverage.fasta"

        # shutil needs str-like src directory until python 3.9
        # https://bugs.python.org/issue32689
        if sys.version_info < (3, 9):
            shutil.move(
                str(wdir / "medaka_polished" / "consensus.fasta"),
                wdir / fasta
            )
        else:
            shutil.move(
                wdir / "medaka_polished" / "consensus.fasta",
                wdir / fasta
            )
        logger.info("  Renamed assembly to reflect pipeline choices.")

class FinalCleanStep(PipelineStep):
    def __init__(self) -> None:
        pass

    def run(self, wdir: str) -> None:
        (wdir / f"{wdir.stem}.fastq.gz").unlink()
        for entry in (wdir / "original").iterdir():
            if sys.version_info < (3, 9):
                shutil.move(str(entry), wdir)
            else:
                shutil.move(entry, wdir)
        try:
            (wdir / "original").rmdir()
        except OSError:
            print("Couldn't delete copy of original files")
        logging.info("Final cleanup done.")


#################### Auxillary #####################


def _get_duplex_call(threads, prefix):
    duplex = [
        "duplex_tools",
        "split_on_adapter",
        f"{prefix}",
        f"{prefix / f'{prefix.stem}_split'}",
        "Native",
        "--threads",
        f"{threads}"
    ]
    duplex_env = model.get_prefix("duplex-tools")
    env = {"PATH": f"{duplex_env}:{os.environ['PATH']}"}
    return duplex, env


def _get_filtlong_call(min_len, target_bases, prefix):
    filtlong = [
        "filtlong",
        "--min_length",
        f"{min_len}",
        "--keep_percent 90",
        "--target_bases",
        f"{target_bases}",
        f"{prefix / f'{prefix.stem}.fastq.gz'}"
    ]
    filtlong_env = model.get_prefix("filtlong")
    env = {"PATH": f"{filtlong_env}:{os.environ['PATH']}"}
    return filtlong, env


def _get_flye_call(threads, prefix):
    filtered_reads = (
        prefix / 'filtered_reads' / f'{prefix.stem}_filtered.fastq.gz'
    )
    flye = [
        "flye",
        "-o",
        f"{prefix / f'{prefix.stem}_flye_assembly'}",
        "--threads", f"{threads}",
        "--nano-hq",
        f"{filtered_reads}"
    ]
    fly_env = model.get_prefix('flye')
    env = {"PATH": f"{fly_env}:{os.environ['PATH']}"}
    return flye, env


def _get_raven_call(threads, prefix):
    filtered_reads = (
        prefix / 'filtered_reads' / f'{prefix.stem}_filtered.fastq.gz'
    )
    raven = [
        "raven",
        "--threads", f"{threads}",
        f"{filtered_reads}"
    ]
    raven_env = model.get_prefix('raven-assembler')
    env = {"PATH": f"{raven_env}:{os.environ['PATH']}"}
    return raven, env


def _get_minimap_overlap(threads, prefix):
    filtered_reads = (
        prefix / 'filtered_reads' / f'{prefix.stem}_filtered.fastq.gz'
    )
    minimap = [
        "minimap2",
        "-x", "ava-ont",
        "-t", f"{threads}",
        f"{filtered_reads}", f"{filtered_reads}"
    ]
    minimap_env = model.get_prefix('minimap2')
    env = {"PATH": f"{minimap_env}:{os.environ['PATH']}"}
    return minimap, env


def _get_minimap_mapping(threads, prefix):
    filtered_reads = (
        prefix / 'filtered_reads' / f'{prefix.stem}_filtered.fastq.gz'
    )
    assembly = (
        prefix / f'{prefix.stem}_flye_assembly' / 'assembly.fasta'
    )
    minimap = [
        "minimap2",
        "-ax", "map-ont",
        "-t", f"{threads}",
        f"{assembly}", f"{filtered_reads}"
    ]
    minimap_env = model.get_prefix('minimap2')
    env = {"PATH": f"{minimap_env}:{os.environ['PATH']}"}
    return minimap, env


def _get_miniasm_call(threads, prefix):
    filtered_reads = (
        prefix / "filtered_reads" / f"{prefix.stem}_filtered.fastq.gz"
    )
    asm_dir = prefix / f"{prefix.stem}_miniasm_assembly"
    overlap = asm_dir / f"{prefix.stem}_overlap.paf.gz"
    miniasm = ["miniasm", "-f", filtered_reads, overlap]
    miniasm_env = model.get_prefix('miniasm')
    env = {"PATH": f"{miniasm_env}:{os.environ['PATH']}"}
    return miniasm, env


def _get_minipolish_call(threads, prefix):
    filtered_reads = (
        prefix / "filtered_reads" / f"{prefix.stem}_filtered.fastq.gz"
    )
    asm_dir = prefix / f"{prefix.stem}_miniasm_assembly"
    assembly = asm_dir / f"{prefix.stem}_unpolished_assembly.gfa"
    minipolish = ["minipolish", "-t", f"{threads}", filtered_reads, assembly]
    minipolish_env = model.get_prefix('minipolish')
    env = {"PATH": f"{minipolish_env}:{os.environ['PATH']}"}
    return minipolish, env


def _get_racon_call(threads, prefix):
    filtered_reads = (
        prefix / 'filtered_reads' / f'{prefix.stem}_filtered.fastq.gz'
    )
    racon = [
        "racon",
        "-m", "8", "-x", "-6",
        "-g", "-8", "-w", "500",
        "--threads", f"{threads}",
        f"{filtered_reads}",
        f"{prefix / 'nanopore_mapping' / 'mapping.sam'}",
        f"{prefix / f'{prefix.stem}_flye_assembly' / 'assembly.fasta'}"
    ]
    racon_env = model.get_prefix('racon')
    env = {"PATH": f"{racon_env}:{os.environ['PATH']}"}
    return racon, env


def _get_medaka_call(threads, assembler, mod, is_racon, prefix):
    filtered_reads = (
        prefix / 'filtered_reads' / f'{prefix.stem}_filtered.fastq.gz'
    )
    fasta_folder = None
    if assembler == "Flye":
        if is_racon:
            fasta_folder = f"{prefix.stem}_racon_polishing"
        else:
            fasta_folder = f"{prefix.stem}_flye_assembly"
    elif assembler == "Raven":
        fasta_folder = f"{prefix.stem}_raven_assembly"
    elif assembler == "Miniasm":
        fasta_folder = f"{prefix.stem}_miniasm_assembly"
    else:
        raise NotImplementedError(
            f"The assembly method {assembler} is not supported."
        )
    fasta = prefix / fasta_folder / "assembly.fasta"
    medaka = [
        "medaka_consensus",
        "-i", f"{filtered_reads}",
        "-d", f"{fasta}",
        "-o", f"{prefix / 'medaka_polished'}",
        "-t", f"{threads}",
        "-m", f"{mod}"
    ]
    medaka_env = model.get_prefix('medaka')
    env = {"PATH": f"{medaka_env}:{os.environ['PATH']}"}
    return medaka, env
