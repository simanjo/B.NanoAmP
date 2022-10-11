import sys
import shutil
import tarfile
import gzip
import pytest
import requests

from PipelineSteps import DuplexStep, CleanDuplexStep
from PipelineSteps import FilterStep, CleanFilterStep
from PipelineSteps import AssemblyStep
from PipelineSteps import RaconPolishingStep, MedakaPolishingStep


@pytest.fixture(scope="session")
def get_fastq_test_data(tmp_path_factory):
    # download test fastq data, extract and remove after test completion
    domain = ".".join([
        "ont-exd-int-s3-euwst1-epi2me-labs",
        "s3-eu-west-1", "amazonaws", "com"
    ])
    url = "https://" + domain + "/fast_introduction/"
    req = requests.get(url + "archive.tar.gz")
    assert req.status_code == 200

    fastq_data = tmp_path_factory.mktemp("tmp_data") / "fastq"
    fastq_data.mkdir()
    with open(fastq_data / "archive.tar.gz", 'wb') as fh:
        fh.write(req.content)
    with tarfile.open(fastq_data / "archive.tar.gz", 'r:gz') as tarfh:
        with open(fastq_data / "example1.fastq", 'wb') as fh:
            fh.write(tarfh.extractfile("test0/fail/example1.fastq").read())
        with open(fastq_data / "example2.fastq", 'wb') as fh:
            fh.write(tarfh.extractfile("test0/pass/example2.fastq").read())
        with gzip.open(fastq_data / "example3.fastq.gz", 'wb') as fh:
            fh.write(tarfh.extractfile("test0/example3.fastq").read())
        print(fastq_data)
    yield fastq_data
    shutil.rmtree(fastq_data)


@pytest.fixture(params=["single", "multiple", "mix"])
def setup_fastq_data(get_fastq_test_data, request):
    data = get_fastq_test_data.parent / "data"
    data.mkdir()
    if request.param == "single":
        shutil.copy(get_fastq_test_data / "example1.fastq", data)
    elif request.param == "multiple":
        shutil.copy(get_fastq_test_data / "example1.fastq", data)
        shutil.copy(get_fastq_test_data / "example2.fastq", data)
    elif request.param == "mix":
        shutil.copy(get_fastq_test_data / "example1.fastq", data)
        shutil.copy(get_fastq_test_data / "example3.fastq.gz", data)
    yield data
    if request.param == "single":
        (data / "example1.fastq").unlink()
        data.rmdir()
    elif request.param == "multiple":
        (data / "example1.fastq").unlink()
        (data / "example2.fastq").unlink()
        data.rmdir()
    elif request.param == "mix":
        (data / "example1.fastq").unlink()
        (data / "example3.fastq.gz").unlink()
        data.rmdir()


@pytest.fixture
def duplex_step(setup_fastq_data, request):
    yield DuplexStep(threads=8)
    clean = request.node.get_closest_marker("clean").args[0]
    if not clean:
        shutil.rmtree(setup_fastq_data / f"{setup_fastq_data.stem}_split")
    (setup_fastq_data / f"{setup_fastq_data.stem}.fastq.gz").unlink()
    for entry in (setup_fastq_data / "original").iterdir():
        # shutil needs str-like src directory until python 3.9
        # https://bugs.python.org/issue32689
        if sys.version_info < (3, 9):
            shutil.move(str(entry), setup_fastq_data)
        else:
            shutil.move(entry, setup_fastq_data)
    (setup_fastq_data / "original").rmdir()


@pytest.fixture
def duplex_clean():
    yield CleanDuplexStep()


@pytest.fixture(params=[(1_000, 4_200_000)])
# TODO: clarify suitable parameters
def filter_step(setup_fastq_data, request):
    min_len, bases = request.param
    yield FilterStep(min_len, bases)
    clean = request.node.get_closest_marker("clean").args[0]
    if not clean:
        shutil.rmtree(setup_fastq_data / "filtered_reads")


@pytest.fixture
def filter_clean():
    yield CleanFilterStep()


@pytest.fixture
def assembly_step(setup_fastq_data, request):
    yield AssemblyStep(threads=8, assembler=request.param)
    clean = request.node.get_closest_marker("clean").args[0]
    if not clean:
        asm_dir = f"{setup_fastq_data.stem}_{request.param.lower()}_assembly"
        shutil.rmtree(setup_fastq_data / asm_dir)


@pytest.fixture
def racon_step(setup_fastq_data, request):
    yield RaconPolishingStep(threads=8)
    clean = request.node.get_closest_marker("clean").args[0]
    if not clean:
        # racon step is built but not always used (if assembler unequals flye)
        # so cleanup is not always required
        shutil.rmtree(
            setup_fastq_data / "nanopore_mapping", ignore_errors=True
        )
        racon_dir = f"{setup_fastq_data.stem}_racon_polishing"
        shutil.rmtree(setup_fastq_data / racon_dir, ignore_errors=True)


@pytest.fixture
def medaka_step(setup_fastq_data, request):
    yield MedakaPolishingStep(
        threads=8, assembler=request.param[0],
        model="r941_min_hac_g507", is_racon=request.param[1]
    )
    clean = request.node.get_closest_marker("clean").args[0]
    if not clean:
        shutil.rmtree(setup_fastq_data / "medaka_polished")


@pytest.mark.clean(False)
@pytest.mark.needs_conda
def test_duplex_step_output(duplex_step, setup_fastq_data):
    duplex_step.run(setup_fastq_data)

    assert (setup_fastq_data / f"{setup_fastq_data.stem}.fastq.gz").is_file()

    split_dir = setup_fastq_data / f"{setup_fastq_data.stem}_split"
    assert split_dir.is_dir()
    assert (split_dir / "unedited.pkl").is_file()
    assert (split_dir / "edited.pkl").is_file()
    assert (split_dir / "split_multiple_times.pkl").is_file()

    orig_dir = setup_fastq_data / "original"
    assert orig_dir.is_dir()
    for entry in orig_dir.iterdir():
        assert entry.is_file()
        if ".gz" in entry.suffixes:
            base = entry.with_suffix("").stem
        else:
            base = entry.stem
        assert (split_dir / f"{base}_split.fastq.gz").is_file()


@pytest.mark.clean(True)
@pytest.mark.needs_conda
def test_duplex_step_cleanup(duplex_step, duplex_clean, setup_fastq_data):
    duplex_step.run(setup_fastq_data)

    split_dir = setup_fastq_data / f"{setup_fastq_data.stem}_split"
    orig_dir = setup_fastq_data / "original"
    assert split_dir.is_dir()
    assert orig_dir.is_dir()
    for entry in orig_dir.iterdir():
        assert entry.is_file()
        if ".gz" in entry.suffixes:
            base = entry.with_suffix("").stem
        else:
            base = entry.stem
        assert (split_dir / f"{base}_split.fastq.gz").is_file()

    duplex_clean.run(setup_fastq_data)
    assert not split_dir.is_dir()
    assert (setup_fastq_data / f"{setup_fastq_data.stem}.fastq.gz").is_file()


@pytest.mark.clean(False)
@pytest.mark.needs_conda
def test_filter_step_output(duplex_step, filter_step, setup_fastq_data):
    duplex_step.run(setup_fastq_data)
    filter_step.run(setup_fastq_data)

    filter_dir = setup_fastq_data / "filtered_reads"
    assert filter_dir.is_dir()
    for entry in filter_dir.iterdir():
        assert entry.name == f"{setup_fastq_data.stem}_filtered.fastq.gz"


@pytest.mark.clean(True)
@pytest.mark.needs_conda
def test_filter_step_cleanup(
    duplex_step, duplex_clean, filter_step, filter_clean, setup_fastq_data
):
    duplex_step.run(setup_fastq_data)
    filter_step.run(setup_fastq_data)
    duplex_clean.run(setup_fastq_data)

    filter_dir = setup_fastq_data / "filtered_reads"
    assert filter_dir.is_dir()
    filter_clean.run(setup_fastq_data)
    assert not filter_dir.is_dir()


@pytest.mark.clean(False)
@pytest.mark.needs_conda
@pytest.mark.parametrize(
    "assembly_step, medaka_step, assembler, racon",
    [
        (asm, (asm, rac), asm.lower(), rac)
        for asm in ["Miniasm", "Flye", "Raven"]
        for rac in [True, False]
    ],
    indirect=["assembly_step", "medaka_step"]
)
def test_assembly_step_output(
    duplex_step, filter_step, assembly_step,
    racon_step, medaka_step, setup_fastq_data,
    assembler, racon
):
    # TODO: flye is not running, adapt test parameters accordingly
    duplex_step.run(setup_fastq_data)
    filter_step.run(setup_fastq_data)
    assembly_step.run(setup_fastq_data)

    asm_dir = f"{setup_fastq_data.stem}_{assembler}_assembly"
    assert (setup_fastq_data / asm_dir).is_dir()

    files = []
    if assembler == "miniasm":
        files = [
            f"{setup_fastq_data.stem}_overlap.paf.gz",
            f"{setup_fastq_data.stem}_unpolished_assembly.gfa",
            f"{setup_fastq_data.stem}_assembly.gfa"
        ]
    elif assembler == "flye":
        files = [
            "flye.log",
            "00-assembly",
            "params.json"
        ]

    files.append("assembly.fasta")
    for entry in (setup_fastq_data / asm_dir).iterdir():
        assert entry.name in files
    # TODO: no assembly in test case for flye
    if assembler == "flye":
        assert not (setup_fastq_data / asm_dir / "assembly.fasta").is_file()
    else:
        assert (setup_fastq_data / asm_dir / "assembly.fasta").is_file()

    # the medaka step runs with racon flag, regardless
    # of assembler choice. Racon is but only run with flye
    if racon and assembler == "flye":
        racon_step.run(setup_fastq_data)
        racon_dir = f"{setup_fastq_data.stem}_racon_polishing"
        map_dir = "nanopore_mapping"
        assert (setup_fastq_data / map_dir).is_dir()
        assert (setup_fastq_data / racon_dir).is_dir()
        assert (setup_fastq_data / map_dir / "mapping.sam").is_file()
        assert (setup_fastq_data / racon_dir / "assembly.fasta").is_file()

    medaka_step.run(setup_fastq_data)
    assert (setup_fastq_data / "medaka_polished").is_dir()
    for entry in (setup_fastq_data / "medaka_polished").iterdir():
        # TODO: dir is emtpy, medaka is not running...
        assert False
