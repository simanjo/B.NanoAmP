import gzip
import os
import shutil
import subprocess
from abc import ABC, abstractmethod

import model


class PipelineStep(ABC):

    @abstractmethod
    def run(self, wdir: str) -> None:
        pass


class DuplexStep(PipelineStep):

    def __init__(self, threads) -> None:
        self.threads = threads

    def run(self, wdir):
        duplex_call, env = _get_duplex_call(self.threads, wdir)
        # print(f"running {duplex_call}")
        proc = subprocess.run(
            duplex_call, capture_output=True, env=env
        )
        if proc.returncode == 0:
            print(proc.returncode)
            print(proc.stdout.decode())
        else:
            print(proc.returncode)
            print(proc.stdout.decode())
            print(proc.stderr.decode())

        orig_dir = os.path.join(wdir, "original")
        os.makedirs(orig_dir)
        for entry in os.scandir(wdir):
            if entry.is_file() and entry.name.endswith("fastq.gz"):
                # shutil needs str-like src directory until python 3.9
                # https://bugs.python.org/issue32689
                shutil.move(entry.path, orig_dir)

        # copy everything
        dirname = os.path.split(wdir)[1]
        outfile = os.path.join(
            wdir, f"{dirname}.fastq.gz"
        )
        split_dir = os.path.join(wdir, f"{dirname}_split")
        with gzip.open(outfile, 'wb') as out_fh:
            for entry in os.scandir(split_dir):
                if entry.is_file() and entry.name.endswith("fastq.gz"):
                    with gzip.open(entry.path, 'rb') as in_fh:
                        shutil.copyfileobj(in_fh, out_fh)


class CleanDuplexStep(PipelineStep):
    def __init__(self) -> None:
        pass

    def run(self, wdir):
        dirname = os.path.split(wdir)[1]
        shutil.rmtree(os.path.join(wdir, f"{dirname}_split"))


class FilterStep(PipelineStep):

    def __init__(self, min_len, target_bases) -> None:
        self.min_len = min_len
        self.target_bases = target_bases

    def run(self, wdir: str) -> None:
        filtered_dir = os.path.join(wdir, "filtered_reads")
        os.makedirs(filtered_dir)

        dirname = os.path.split(wdir)[1]
        filtlong_call, env = _get_filtlong_call(
            self.min_len, self.target_bases, wdir
        )
        # print(f"running {filtlong_call}")
        proc = subprocess.Popen(
            filtlong_call, stdout=subprocess.PIPE, env=env
        )
        filtered_reads = os.path.join(
            filtered_dir, f"{dirname}_filtered.fastq.gz"
        )
        with gzip.open(filtered_reads, 'wb') as out_fh:
            # ugly HACK? Is this supposed to be ok?
            # use stdout PIPE as filelike input to copy
            # implicitly calls communicate?
            shutil.copyfileobj(proc.stdout, out_fh)


class CleanFilterStep(PipelineStep):
    def __init__(self) -> None:
        pass

    def run(self, wdir):
        shutil.rmtree(os.path.join(wdir, "filtered_reads"))


class AssemblyStep(PipelineStep):

    def __init__(self, threads, assembler) -> None:
        self.threads = threads
        self.assembler = assembler

    def run(self, wdir: str) -> None:
        if self.assembler == "Miniasm":
            dirname = os.path.split(wdir)[1]
            asm_dir = os.path.join(wdir, f"{dirname}_miniasm_assembly")
            os.makedirs(asm_dir)

            minimap_call, env = _get_minimap_overlap(
                self.threads, wdir
            )
            proc = subprocess.Popen(
                minimap_call, stdout=subprocess.PIPE, env=env
            )
            read_overlap = os.path.join(
                asm_dir, f"{dirname}_overlap.paf.gz"
            )
            with gzip.open(read_overlap, 'wb') as out_fh:
                shutil.copyfileobj(proc.stdout, out_fh)

            assembler_call, env = _get_miniasm_call(
                self.threads, wdir
            )
            proc = subprocess.Popen(
                assembler_call, stdout=subprocess.PIPE, env=env
            )
            asm_output = os.path.join(
                asm_dir, f"{dirname}_unpolished_assembly.gfa"
            )
            with open(asm_output, 'wb') as out_fh:
                shutil.copyfileobj(proc.stdout, out_fh)

            polish_call, env = _get_minipolish_call(
                self.threads, wdir
            )
            proc = subprocess.Popen(
                polish_call, stdout=subprocess.PIPE, env=env
            )
            polish_output = os.path.join(
                asm_dir, f"{dirname}_assembly.gfa"
            )
            with open(polish_output, 'wb') as out_fh:
                shutil.copyfileobj(proc.stdout, out_fh)

            ################# HACK
            # cf awk '/^S/{print ">"$2"\n"$3}' assmb.gfa | fold > assmb.fasta
            fasta_conv_awk = subprocess.Popen(
                ["awk", "/^S/{print \">\"$2\"\\n\"$3}", polish_output],
                stdout=subprocess.PIPE
            )
            fasta_conv_fold = subprocess.Popen(
                ["fold"], stdin=fasta_conv_awk.stdout, stdout=subprocess.PIPE
            )
            asm_output = os.path.join(
                asm_dir, "assembly.fasta"
            )
            with open(asm_output, 'wb') as out_fh:
                shutil.copyfileobj(fasta_conv_fold.stdout, out_fh)
            ######################

        elif self.assembler == "Flye":
            assembler_call, env = _get_flye_call(
                self.threads, wdir
            )
            # print(f"running {assembler_call}")
            proc = subprocess.run(
                assembler_call, capture_output=True, env=env
            )
            if proc.returncode == 0:
                print(proc.returncode)
                print(proc.stdout.decode())
            else:
                print(proc.returncode)
                print(proc.stdout.decode())
                print(proc.stderr.decode())

        elif self.assembler == "Raven":
            dirname = os.path.split(wdir)[1]
            asm_dir = os.path.join(wdir, f"{dirname}_raven_assembly")
            os.makedirs(asm_dir)
            assembler_call, env = _get_raven_call(
                self.threads, wdir
            )
            # print(f"running {assembler_call}")
            proc = subprocess.Popen(
                assembler_call, stdout=subprocess.PIPE, env=env
            )
            asm_output = os.path.join(asm_dir, "assembly.fasta")
            with open(asm_output, 'wb') as out_fh:
                shutil.copyfileobj(proc.stdout, out_fh)

        else:
            raise NotImplementedError(
                f"The assembly method {self.assembler} is not supported."
            )


class CleanAssemblyStep(PipelineStep):
    def __init__(self, assembler, is_racon) -> None:
        self.assembler = assembler
        self.is_racon = is_racon and assembler == "Flye"

    def run(self, wdir):
        dirname = os.path.split(wdir)[1]
        asm_name = f"{dirname}_{self.assembler.lower()}_assembly"
        shutil.rmtree(os.path.join(wdir, asm_name))

        if self.is_racon:
            mapping_dir = os.path.join(wdir, "nanopore_mapping")
            polish_dir = os.path.join(
                wdir, f"{dirname}_racon_polishing"
            )
            shutil.rmtree(mapping_dir)
            shutil.rmtree(polish_dir)

        medaka_dir = os.path.join(wdir, "medaka_polished")
        shutil.rmtree(medaka_dir)

        asm_dir = os.path.join(wdir, "assemblies")
        if not os.path.isdir(asm_dir):
            os.makedirs(asm_dir)
        polish_flag = "rm" if self.is_racon else "m"
        fasta_name = f"{dirname}_{self.assembler}_{polish_flag}_coverage.fasta"
        if (os.path.isfile(os.path.join(wdir, fasta_name))):
            shutil.move(os.path.join(wdir, fasta_name), asm_dir)


class RaconPolishingStep(PipelineStep):
    def __init__(self, threads) -> None:
        self.threads = threads

    def run(self, wdir):
        dirname = os.path.split(wdir)[1]
        mapping_dir = os.path.join(wdir, "nanopore_mapping")
        os.makedirs(mapping_dir)
        polish_dir = os.path.join(wdir, f"{dirname}_racon_polishing")
        os.makedirs(polish_dir)

        minimap_call, env = _get_minimap_mapping(
            self.threads, wdir
        )
        # print(f"running {minimap_call}")
        proc = subprocess.Popen(
            minimap_call, stdout=subprocess.PIPE, env=env
        )
        mapping = os.path.join(
            mapping_dir, "mapping.sam"
        )
        with open(mapping, 'wb') as out_fh:
            # ugly HACK?
            # use stdout PIPE as filelike input to copy
            # implicitly calls communicate?
            shutil.copyfileobj(proc.stdout, out_fh)

        polishing_call, env = _get_racon_call(
            self.threads, wdir
        )
        # print(f"running {polishing_call}")
        proc = subprocess.Popen(
            polishing_call, stdout=subprocess.PIPE, env=env
        )
        polish_out = os.path.join(
            polish_dir, "assembly.fasta"
        )
        with open(polish_out, 'wb') as out_fh:
            # ugly HACK?
            # use stdout PIPE as filelike input to copy
            # implicitly calls communicate?
            shutil.copyfileobj(proc.stdout, out_fh)


class MedakaPolishingStep(PipelineStep):
    def __init__(self, threads, assembler, model, is_racon) -> None:
        self.threads = threads
        self.assembler = assembler
        self.model = model
        self.is_racon = is_racon and assembler == "Flye"
        super().__init__()

    def run(self, wdir):
        medaka_call, env = _get_medaka_call(
            self.threads, self.assembler,
            self.model, self.is_racon, wdir
        )
        # print(f"running {medaka_call}")
        proc = subprocess.run(
            medaka_call, capture_output=True, env=env
        )
        if proc.returncode == 0:
            print(proc.returncode)
            print(proc.stdout.decode())

            dirname = os.path.split(wdir)[1]
            polish = "rm" if self.is_racon else "m"
            fasta_name = f"{dirname}_{self.assembler}_{polish}_coverage.fasta"

            shutil.move(
                os.path.join(wdir, "medaka_polished", "consensus.fasta"),
                os.path.join(wdir, fasta_name)
            )

        else:
            print(proc.returncode)
            print(proc.stdout.decode())
            print(proc.stderr.decode())


class FinalCleanStep(PipelineStep):
    def __init__(self) -> None:
        pass

    def run(self, wdir: str) -> None:
        dirname = os.path.split(wdir)[1]
        os.remove(os.path.join(wdir, f"{dirname}.fastq.gz"))
        for entry in os.scandir(os.path.join(wdir, "original")):
            shutil.move(entry.path, wdir)
        try:
            os.rmdir(os.path.join(wdir, "original"))
        except OSError:
            print("Couldn't delete copy of original files")


#################### Auxillary #####################


def _get_duplex_call(threads, prefix):
    dirname = os.path.split(prefix)[1]
    duplex = [
        "duplex_tools",
        "split_on_adapter",
        f"{prefix}",
        f"{os.path.join(prefix, f'{dirname}_split')}",
        "Native",
        "--threads",
        f"{threads}"
    ]
    duplex_env = model.get_prefix("duplex-tools")
    env = {"PATH": f"{duplex_env}:{os.environ['PATH']}"}
    return duplex, env


def _get_filtlong_call(min_len, target_bases, prefix):
    dirname = os.path.split(prefix)[1]
    filtlong = [
        "filtlong",
        "--min_length",
        f"{min_len}",
        "--keep_percent 90",
        "--target_bases",
        f"{target_bases}",
        f"{os.path.join(prefix, f'{dirname}.fastq.gz')}"
    ]
    filtlong_env = model.get_prefix("filtlong")
    env = {"PATH": f"{filtlong_env}:{os.environ['PATH']}"}
    return filtlong, env


def _get_flye_call(threads, prefix):
    dirname = os.path.split(prefix)[1]
    filtered_reads = os.path.join(
        prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz'
    )
    flye = [
        "flye",
        "-o",
        f"{os.path.join(prefix, f'{dirname}_flye_assembly')}",
        "--threads", f"{threads}",
        "--nano-hq",
        f"{filtered_reads}"
    ]
    fly_env = model.get_prefix('flye')
    env = {"PATH": f"{fly_env}:{os.environ['PATH']}"}
    return flye, env


def _get_raven_call(threads, prefix):
    dirname = os.path.split(prefix)[1]
    filtered_reads = os.path.join(
        prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz'
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
    dirname = os.path.split(prefix)[1]
    filtered_reads = os.path.join(
        prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz'
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
    dirname = os.path.split(prefix)[1]
    filtered_reads = os.path.join(
        prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz'
    )
    assembly = os.path.join(
        prefix, f'{dirname}_flye_assembly', 'assembly.fasta'
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
    dirname = os.path.split(prefix)[1]
    filtered_reads = os.path.join(
        prefix, "filtered_reads", f"{dirname}_filtered.fastq.gz"
    )
    asm_dir = os.path.join(prefix, f"{dirname}_miniasm_assembly")
    overlap = os.path.join(asm_dir, f"{dirname}_overlap.paf.gz")
    miniasm = ["miniasm", "-f", filtered_reads, overlap]
    miniasm_env = model.get_prefix('miniasm')
    env = {"PATH": f"{miniasm_env}:{os.environ['PATH']}"}
    return miniasm, env


def _get_minipolish_call(threads, prefix):
    dirname = os.path.split(prefix)[1]
    filtered_reads = os.path.join(
        prefix, "filtered_reads", f"{dirname}_filtered.fastq.gz"
    )
    asm_dir = os.path.join(prefix, f"{dirname}_miniasm_assembly")
    assembly = os.path.join(asm_dir, f"{dirname}_unpolished_assembly.gfa")
    minipolish = ["minipolish", "-t", f"{threads}", filtered_reads, assembly]
    minipolish_env = model.get_prefix('minipolish')
    env = {"PATH": f"{minipolish_env}:{os.environ['PATH']}"}
    return minipolish, env


def _get_racon_call(threads, prefix):
    dirname = os.path.split(prefix)[1]
    filtered_reads = os.path.join(
        prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz'
    )
    racon = [
        "racon",
        "-m", "8", "-x", "-6",
        "-g", "-8", "-w", "500",
        "--threads", f"{threads}",
        f"{filtered_reads}",
        f"{os.path.join(prefix, 'nanopore_mapping', 'mapping.sam')}",
        f"{os.path.join(prefix, f'{dirname}_flye_assembly', 'assembly.fasta')}"
    ]
    racon_env = model.get_prefix('racon')
    env = {"PATH": f"{racon_env}:{os.environ['PATH']}"}
    return racon, env


def _get_medaka_call(threads, assembler, mod, is_racon, prefix):
    dirname = os.path.split(prefix)[1]
    filtered_reads = os.path.join(
        prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz'
    )
    fasta_folder = None
    if assembler == "Flye":
        if is_racon:
            fasta_folder = f"{dirname}_racon_polishing"
        else:
            fasta_folder = f"{dirname}_flye_assembly"
    elif assembler == "Raven":
        fasta_folder = f"{dirname}_raven_assembly"
    elif assembler == "Miniasm":
        fasta_folder = f"{dirname}_miniasm_assembly"
    else:
        raise NotImplementedError(
            f"The assembly method {assembler} is not supported."
        )
    fasta = os.path.join(prefix, fasta_folder, "assembly.fasta")
    medaka = [
        "medaka_consensus",
        "-i", f"{filtered_reads}",
        "-d", f"{fasta}",
        "-o", f"{os.path.join(prefix, 'medaka_polished')}",
        "-t", f"{threads}",
        "-m", f"{mod}"
    ]
    medaka_env = model.get_prefix('medaka')
    env = {"PATH": f"{medaka_env}:{os.environ['PATH']}"}
    return medaka, env
