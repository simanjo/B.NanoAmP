import os
import subprocess

import pandas as pd

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

PREFIXES = {}

############################ Medaka model detection
# medaka_env = {"PATH": model.get_prefix('medaka')+f":{os.environ['PATH']}"}
# proc = subprocess.run(
#     ["medaka", "tools", "list_models"], capture_output=True, env=medaka_env
# )

# if proc.returncode != 0:
#     raise OSError(proc.returncode, proc.stderr.decode())
# allmodels, def_cons, def_var = proc.stdout.decode().splitlines()

# # split allmodels, remove first entry "Available:"
# # and strip trailing "," all but last
# allmodels = [mod[:-1] for mod in allmodels.split()[1:-1]]
#           + [allmodels.split()[-1]]
# cell = [mod.split("_")[0] for mod in allmodels]
# guppy = [mod.split("_")[-1] if mod.split("_")[-1].startswith("g")
#          else mod.split("_")[-2] for mod in allmodels]
# variant = ["_".join(mod.split("_")[1:-1]
#             if mod.split("_")[1] not in ["min", "prom"]
#            else mod.split("_")[2:-1]
#             if mod.split("_")[-1].startswith("g")
#            else mod.split("_")[2:-2]) for mod in allmodels]
########################################################

MODELS = None

# medaka 1.6.1
# set(cell) = {'r103', 'r10', 'r104', 'r1041', 'r941'}
CELLS = {
    "r941": "R9.4.1",
    "r10": "R10",
    "r103": "R10.3",
    "r104": "R10.4",
    "r1041": "R10.4.1"
}

# medaka 1.6.1
# set guppy = {'g340', 'g322', 'g4011', 'g345',
#   'g5015', 'g344', 'g514', 'g507', 'g303', 'g3210', 'g360',
#   'g351', 'g610', 'g330', 'g615'}
GUPPYVERS = {
    "g303": "Guppy 3.0.3",
    "g322": "Guppy 3.2.2",
    "g3210": "Guppy 3.2.10",
    "g330": "Guppy 3.3.0",
    "g340": "Guppy 3.4.0",
    "g344": "Guppy 3.4.4",
    "g345": "Guppy 3.4.5",
    "g351": "Guppy 3.5.1",
    "g360": "Guppy 3.6.0",
    "g4011": "Guppy 4.0.11",
    "g507": "Guppy 5.0.7",
    "g5015": "Guppy 5.0.15",
    "g514": "Guppy 5.1.4",
    "g610": "Guppy 6.1.0",
    "g615": "Guppy 6.1.5",
}

DEVICES = {
    "min": "MinION",
    "prom": "PromethION"
}

# set(variant) = {
#     'fast', 'hac', 'sup',
#     'snp', 'hac_snp', 'fast_snp', 'sup_snp',
#     'variant', 'fast_variant', 'hac_variant', 'sup_variant',
#     'e81_fast', 'e81_hac', 'e81_sup',
#     'e81_fast_variant', 'e81_hac_variant', 'e81_sup_variant',
#     'e82_400bps_fast', 'e82_400bps_hac', 'e82_400bps_sup',
#     'e82_400bps_fast_variant', 'e82_400bps_hac_variant',
#     'e82_400bps_sup_variant',
#     'sup_plant', 'sup_plant_variant', 'high'
# }

VARIANTS = {
    "fast": "fast",
    "hac": "hac",
    "sup": "sup",

    "snp": "snp",
    "fast_snp": "fast_snp",
    "hac_snp": "snp",
    "sup_snp": "sup_snp",

    "variant": "variant",
    "fast_variant": "fast_variant",
    "hac_variant": "hac_variant",
    "sup_variant": "sup_variant",

    "e81_fast": "e81_fast",
    "e81_hac": "e81_hac",
    "e81_sup": "e81_sup",

    "e81_fast_variant": "e81_fast_variant",
    "e81_hac_variant": "e81_hac_variant",
    "e81_sup_variant": "e81_sup_variant",

    "e82_400bps_fast": "e82_400bps_fast",
    "e82_400bps_hac": "e82_400bps_hac",
    "e82_400bps_sup": "e82_400bps_sup",

    "e82_400bps_fast_variant": "e82_400bps_fast_variant",
    "e82_400bps_hac_variant": "e82_400bps_hac_variant",
    "e82_400bps_sup_variant": "e82_400bps_sup_variant",

    "high": "high",
    "sup_plant": "sup_plant",
    "sup_plant_variant": "sup_plant_variant",
}


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


def get_prefix(pkg):
    return PREFIXES[pkg]


def get_flow_cells():
    return ["--"] + list(CELLS.values())


def get_devices():
    return ["--"] + list(DEVICES.values())


def get_guppy_versions():
    return ["--"] + list(GUPPYVERS.values())


def get_guppy_variants():
    return ["--"] + list(VARIANTS.values())


def get_display_names(name, selection):
    if name == "cell":
        df = CELLS
    elif name == "device":
        df = DEVICES
    elif name == "guppy":
        df = GUPPYVERS
    elif name == "variant":
        df = VARIANTS
    else:
        msg = f"The parameter choice {name} is not supported.\n"
        msg += "Try one of 'cell', 'device', 'guppy' or 'variant'."
        raise NotImplementedError(msg)
    print(selection)
    print(df)
    result = list(set([df[item] for item in selection]))
    result.sort()
    return ["--"] + result


def get_param_name(name, val):
    if name == "cell":
        df = CELLS
    elif name == "device":
        df = DEVICES
    elif name == "guppy":
        df = GUPPYVERS
    elif name == "variant":
        df = VARIANTS
    else:
        msg = f"The parameter choice {name} is not supported.\n"
        msg += "Try one of 'cell', 'device', 'guppy' or 'variant'."
        raise NotImplementedError(msg)
    def _get_param(item):
        for key, val in df.items():
            if val == item:
                return key
    return _get_param(val)



def get_models():
    return list(get_model_df()["full_model"])


def get_model_df():
    global MODELS
    if MODELS is None:
        MODELS = _parse_models()
    return MODELS


def _parse_models():
    medaka_env = {"PATH": get_prefix('medaka') + f":{os.environ['PATH']}"}
    proc = subprocess.run(
        ["medaka", "tools", "list_models"], capture_output=True, env=medaka_env
    )

    if proc.returncode != 0:
        raise OSError(proc.returncode, proc.stderr.decode())
    models = proc.stdout.decode().splitlines()[0]

    # split allmodels, remove first entry "Available:"
    # and strip trailing "," all but last
    models = [mod[:-1] for mod in models.split()[1:-1]] + [models.split()[-1]]
    cell = [mod.split("_")[0] for mod in models]
    device = [mod.split("_")[1] if mod.split("_")[1] in ["min", "prom"]
              else "" for mod in models]
    guppy = [mod.split("_")[-1] if mod.split("_")[-1].startswith("g")
             else mod.split("_")[-2] for mod in models]
    variant = ["_".join(
               mod.split("_")[1:-1] if mod.split("_")[1] not in ["min", "prom"]
               else mod.split("_")[2:-1] if mod.split("_")[-1].startswith("g")
               else mod.split("_")[2:-2]
               ) for mod in models]
    return pd.DataFrame({
        "full_model": models,
        "cell": cell,
        "device": device,
        "variant": variant,
        "guppy": guppy
    })


def get_assemblers():
    return ["Flye", "Raven", "Miniasm"]
