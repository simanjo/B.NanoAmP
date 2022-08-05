import os

BINARIES = [
    "duplex-tools",
    "filtlong",
    "flye",
    "raven-assembler",
    "miniasm",
    "minipolish",
    "minimap2",
    "racon",
    "medaka"
    ]

def get_conda_ymls():
    res_dir = os.path.abspath(
        os.path.join(os.path.realpath(__file__), os.path.pardir, "ressources")
    )
    return [
        (
            "nanoamp_assmb",
            os.path.join(res_dir, "cgMLSTassemble_environment")
        ),
        (
            "nanoamp_medaka",
            os.path.join(res_dir, "medaka_environment")
        )
    ]


def get_flow_cells():
    return []

def get_devices():
    return []

def get_guppy_versions():
    return []

def get_models():
    return ["r104_e81_sup_g5015"]

def get_assemblers():
    return ["Flye", "Raven", "Miniasm"]
