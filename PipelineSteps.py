from abc import ABC, abstractmethod

import gzip, os, shutil, subprocess


class PipelineStep(ABC):

    @abstractmethod
    def run(self, working_dir:str) -> None:
        pass

class DuplexStep(PipelineStep):

    def __init__(self, threads) -> None:
        self.threads = threads

    def run(self, working_dir):
        duplex_call = _get_duplex_call(self.threads, working_dir)
        print(f"running {duplex_call}")
        proc = subprocess.run(
            duplex_call, capture_output=True
        )
        if proc.returncode == 0:
            print(proc.returncode)
            print(proc.stdout.decode())
        else:
            print(proc.returncode)
            print(proc.stdout.decode())
            print(proc.stderr.decode())

        orig_dir = os.path.join(working_dir, "original")
        os.makedirs(orig_dir)
        for entry in os.scandir(working_dir):
            if entry.is_file() and entry.name.endswith("fastq.gz"):
                # shutil needs str-like src directory until python 3.9
                # https://bugs.python.org/issue32689
                shutil.move(entry.path, orig_dir)

        # copy everything
        dirname = os.path.split(working_dir)[1]
        outfile = os.path.join(
            working_dir, f"{dirname}.fastq.gz"
        )
        split_dir = os.path.join(working_dir, f"{dirname}_split")
        with gzip.open(outfile, 'wb') as out_fh:
            for entry in os.scandir(split_dir):
                if entry.is_file() and entry.name.endswith("fastq.gz"):
                    with gzip.open(entry.path, 'rb') as in_fh:
                        shutil.copyfileobj(in_fh, out_fh)


class FilterStep(PipelineStep):

    def __init__(self, min_len, target_bases) -> None:
        self.min_len = min_len
        self.target_bases = target_bases

    def run(self, working_dir: str) -> None:
        filtered_dir = os.path.join(working_dir, "filtered_reads")
        os.makedirs(filtered_dir)

        dirname = os.path.split(working_dir)[1]
        filtlong_call = _get_filtlong_call(
            self.min_len, self.target_bases, working_dir
        )
        print(f"running {filtlong_call}")
        proc = subprocess.Popen(
            filtlong_call, stdout=subprocess.PIPE
        )
        filtered_reads = os.path.join(
            filtered_dir, f"{dirname}_filtered.fastq.gz"
        )
        with gzip.open(filtered_reads, 'wb') as out_fh:
            # ugly HACK?
            # use stdout PIPE as filelike input to copy
            # implicitly calls communicate?
            shutil.copyfileobj(proc.stdout, out_fh)


class AssemblyStep(PipelineStep):

    def __init__(self, threads, assembler) -> None:
        self.threads = threads
        self.assembler = assembler

    def run(self, working_dir: str) -> None:
        if self.assembler == "Miniasm":
            dirname = os.path.split(working_dir)[1]
            overlap_dir = os.path.join(working_dir, "read_overlap")
            os.makedirs(overlap_dir)
            asm_dir = os.path.join(working_dir, f"{dirname}_miniasm_assembly")
            os.makedirs(asm_dir)

            print("running minimap2 first")
            minimap_call = _get_minimap_overlap(
                self.threads, working_dir
            )
            print(f"running {minimap_call}")
            proc = subprocess.Popen(
                minimap_call, stdout=subprocess.PIPE
            )
            read_overlap = os.path.join(
                overlap_dir, f"{dirname}_overlap.paf.gz"
            )
            with gzip.open(read_overlap, 'wb') as out_fh:
                # ugly HACK?
                # use stdout PIPE as filelike input to copy
                # implicitly calls communicate?
                shutil.copyfileobj(proc.stdout, out_fh)

            assembler_call = _get_miniasm_call(
                self.threads, working_dir
            )
            print(f"running {assembler_call}")
            proc = subprocess.Popen(
                assembler_call, stdout=subprocess.PIPE
            )
            asm_output = os.path.join(
                asm_dir, f"{dirname}_assembly.gfa"
            )
            with open(asm_output, 'wb') as out_fh:
                # ugly HACK?
                # use stdout PIPE as filelike input to copy
                # implicitly calls communicate?
                shutil.copyfileobj(proc.stdout, out_fh)

        elif self.assembler == "Flye":
            assembler_call, env = _get_flye_call(
                self.threads, working_dir
            )
            print(f"running {assembler_call}")
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
            dirname = os.path.split(working_dir)[1]
            asm_dir = os.path.join(working_dir, f"{dirname}_raven_assembly")
            os.makedirs(asm_dir)
            assembler_call, env = _get_raven_call(
                self.threads, working_dir
            )
            print(f"running {assembler_call}")
            proc = subprocess.Popen(
                assembler_call, stdout=subprocess.PIPE, env=env
            )
            asm_output = os.path.join(asm_dir, "assembly.fasta")
            with open(asm_output, 'wb') as out_fh:
                # ugly HACK?
                # use stdout PIPE as filelike input to copy
                # implicitly calls communicate?
                shutil.copyfileobj(proc.stdout, out_fh)

        else:
            raise NotImplemented(
                f"The assembly method {self.assembler} is not supported."
            )

class RaconPolishingStep(PipelineStep):
    def __init__(self, threads) -> None:
        self.threads = threads

    def run(self, working_dir):
        dirname = os.path.split(working_dir)[1]
        mapping_dir = os.path.join(working_dir, "nanopore_mapping")
        os.makedirs(mapping_dir)
        polish_dir = os.path.join(working_dir, f"{dirname}_racon_polishing")
        os.makedirs(polish_dir)

        print("running minimap2 first")
        minimap_call = _get_minimap_mapping(
            self.threads, working_dir
        )
        print(f"running {minimap_call}")
        proc = subprocess.Popen(
            minimap_call, stdout=subprocess.PIPE
        )
        mapping = os.path.join(
            mapping_dir, "mapping.sam"
        )
        with open(mapping, 'wb') as out_fh:
            # ugly HACK?
            # use stdout PIPE as filelike input to copy
            # implicitly calls communicate?
            shutil.copyfileobj(proc.stdout, out_fh)

        polishing_call = _get_racon_call(
            self.threads, working_dir
        )
        print(f"running {polishing_call}")
        proc = subprocess.Popen(
            polishing_call, stdout=subprocess.PIPE
        )
        polish_out = os.path.join(
           polish_dir, "racon.fasta"
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
        self.is_racon = is_racon
        super().__init__()

    def run(self, working_dir):
        medaka_call, env = _get_medaka_call(
            self.threads, self.assembler,
            self.model, self.is_racon, working_dir
        )
        print(f"running {medaka_call}")
        proc = subprocess.run(
            medaka_call, capture_output=True, env=env
        )
        if proc.returncode == 0:
            print(proc.returncode)
            print(proc.stdout.decode())

            dirname = os.path.split(working_dir)[1]
            polish_flag = "rm" if self.is_racon else "m"
            fasta_name = f"{dirname}_{self.assembler}_{polish_flag}_coverage.fasta"

            shutil.copyfile(
                os.path.join(working_dir, "medaka_polished", "consensus.fasta"),
                os.path.join(working_dir, fasta_name)
            )
            
        else:
            print(proc.returncode)
            print(proc.stdout.decode())
            print(proc.stderr.decode())



#################### Auxillary #####################

def _get_duplex_call(threads, prefix):
    duplex_bin = model.BINARIES['duplex-tools']['bin']
    dirname = os.path.split(prefix)[1]
    duplex = [
        duplex_bin,
        "split_on_adapter",
        f"{prefix}",
        f"{os.path.join(prefix, f'{dirname}_split')}",
        "Native",
        "--threads",
        f"{threads}"
    ]
    return duplex

def _get_filtlong_call(min_len, target_bases, prefix):
    filtlong_bin = model.BINARIES['filtlong']['bin']
    dirname = os.path.split(prefix)[1]
    filtlong = [
        filtlong_bin,
        "--min_length",
        f"{min_len}",
        "--keep_percent 90",
        "--target_bases",
        f"{target_bases}",
        f"{os.path.join(prefix, f'{dirname}.fastq.gz')}"
    ]
    return filtlong

def _get_flye_call(threads, prefix):
    flye_bin = model.BINARIES['flye']['bin']
    dirname = os.path.split(prefix)[1]
    flye = [
        flye_bin,
        "-o",
        f"{os.path.join(prefix, f'{dirname}_flye_assembly')}",
        "--threads", f"{threads}",
        "--nano-hq",
        f"{os.path.join(prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz')}"
    ]
    fly_env = os.path.join(model.BINARIES['flye']['prefix'], "bin")
    env = {"PATH": f"{fly_env}:{os.environ['PATH']}"}
    return flye, env

def _get_raven_call(threads, prefix):
    raven_bin = model.BINARIES['raven-assembler']['bin']
    dirname = os.path.split(prefix)[1]
    raven = [
        raven_bin,
        "--threads", f"{threads}",
        f"{os.path.join(prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz')}"
    ]
    raven_env = os.path.join(model.BINARIES['raven-assembler']['prefix'], "bin")
    env = {"PATH": f"{raven_env}:{os.environ['PATH']}"}
    return raven, env

def _get_minimap_overlap(threads, prefix):
    minimap_bin = model.BINARIES['minimap2']['bin']
    dirname = os.path.split(prefix)[1]
    minimap = [
        minimap_bin,
        "-x", "ava-ont",
        "-t", f"{threads}",
        f"{os.path.join(prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz')}",
        f"{os.path.join(prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz')}"
    ]
    return minimap

def _get_minimap_mapping(threads, prefix):
    minimap_bin = model.BINARIES['minimap2']['bin']
    dirname = os.path.split(prefix)[1]
    minimap = [
        minimap_bin,
        "-ax", "map-ont",
        "-t", f"{threads}",
        f"{os.path.join(prefix, f'{dirname}_flye_assembly', 'assembly.fasta')}",
        f"{os.path.join(prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz')}"
    ]
    return minimap

def _get_miniasm_call(threads, prefix):
    miniasm_bin = model.BINARIES['miniasm']['bin']
    dirname = os.path.split(prefix)[1]
    miniasm = [
        miniasm_bin,
        "-f",
        f"{os.path.join(prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz')}",
        f"{os.path.join(prefix, 'read_overlap', f'{dirname}_overlap.paf.gz')}"
    ]
    return miniasm

def _get_racon_call(threads, prefix):
    racon_bin = model.BINARIES['racon']['bin']
    dirname = os.path.split(prefix)[1]
    racon = [
        racon_bin,
        "-m", "8", "-x", "-6",
        "-g", "-8", "-w", "500",
        "--threads", f"{threads}",
        f"{os.path.join(prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz')}",
        f"{os.path.join(prefix, 'nanopore_mapping', 'mapping.sam')}",
        f"{os.path.join(prefix, f'{dirname}_flye_assembly', 'assembly.fasta')}"
    ]
    return racon

def _get_medaka_call(threads, assembler, mod, is_racon, prefix):
    medaka_bin = model.BINARIES['medaka']['bin']
    dirname = os.path.split(prefix)[1]
    fasta = None
    if assembler == "Flye":
        if is_racon:
            fasta = os.path.join(prefix, "racon_polishing", "racon.fasta")
        else:
            fasta = os.path.join(prefix, f"{dirname}_flye_assembly", "assembly.fasta")
    elif assembler == "Raven":
        fasta = os.path.join(prefix, f"{dirname}_raven_assembly", "assembly.fasta")
    else:
        raise NotImplemented(
            f"The assembly method {assembler} is not supported."
        )

    medaka = [
        medaka_bin,
        "-i",
        f"{os.path.join(prefix, 'filtered_reads', f'{dirname}_filtered.fastq.gz')}",
        "-d", f"{fasta}",
        "-o", f"{os.path.join(prefix, 'medaka_polished')}",
        "-t", f"{threads}",
        "-m", f"{mod}"
    ]
    medaka_env = os.path.join(model.BINARIES['medaka']['prefix'], "bin")
    env = {"PATH": f"{medaka_env}:{os.environ['PATH']}"}
    return medaka, env
