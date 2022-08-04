import os

import dearpygui.dearpygui as dpg

import model, controller

def add_file_dialog():
    dpg.add_file_dialog(
        label="Select Base Folder",
        directory_selector=True, show=False,
        callback=_choose_dir, tag="file_dialog",
        width=500, height=400
    )

def add_main_window():
    with dpg.window(tag="main_window", autosize=True, no_close=True, no_collapse=True):
        with dpg.tab_bar():
            with dpg.tab(label="Assembly Settings", tag="main_tab"):
                _add_general_settings()
                _add_filtlong_settings()
                _add_assembler_settings()
                _add_medaka_settings()
                dpg.add_button(
                    label="Execute Assembly Pipeline",
                    callback=controller.execute_pipeline
                )


#################### Auxillary ####################

def _choose_dir(sender, app_data) -> None:

    try:
        fpath = list(app_data['selections'].values())[0]
    except KeyError:
        return

    # HACK/TODO: dir chooser is buggy
    # check path and strip last part if necessary
    if not os.path.isdir(fpath):
        fpath = os.path.abspath(
            os.path.join(fpath, os.pardir)
        )
    assert os.path.isdir(fpath)

def _add_general_settings():
    dpg.add_text("General Settings")
    with dpg.group(horizontal=True):
        dpg.add_button(
            label="Select Base Folder",
            callback=lambda: dpg.show_item("file_dialog")
        )
        dpg.add_text(tag="bcfolder", show=True, default_value="")
    dpg.add_checkbox(
        label="skip assembly in unclassified folder",
        tag="skip_unclassified",
        default_value=True
    )
    dpg.add_input_int(
        label="Threads", tag="threads", default_value=8
    )
    dpg.add_input_float(
        label="Genome Size [MB]", tag="genome_size", default_value=4.2
    )
    dpg.add_input_int(
        label="Coverage", tag="coverage", default_value=100
    )
    dpg.add_separator()

def _add_filtlong_settings():
    dpg.add_text("Filtlong Settings")
    dpg.add_input_int(
        label="min_length",
        tag="filtlong_minlen",
        default_value=1_000
    )
    dpg.add_separator()

def _add_assembler_settings():
    dpg.add_text("Assembler Settings:")
    with dpg.group(horizontal=True):
        dpg.add_checkbox(
            label="Flye",
            tag="use_flye",
            default_value=True,
            callback=_toggle_flye
        )
        dpg.add_checkbox(
            label="skip racon polishing",
            tag="racon_skip", default_value=True
        )
    dpg.add_checkbox(
        label="Raven",
        tag="use_raven",
        default_value=False
    )
    dpg.add_checkbox(
        label="Miniasm",
        tag="use_miniasm",
        default_value=False
    )
    dpg.add_separator()

def _add_medaka_settings():
    dpg.add_text("Medaka Settings:")
    dpg.add_checkbox(
        label="choose model manually",
        tag="medaka_choose", default_value=False,
        callback=_toggle_medaka_model
    )
    dpg.add_combo(
        label="Model", tag="medaka_manumodel",
        default_value="r104_e81_sup_g5015", items=model.get_models(),
        show=False
    )
    with dpg.group(tag="medaka_automodel"):
        dpg.add_combo(
            label="FlowCell", tag="medaka_flowcell",
            default_value="", items=model.get_flow_cells()
        )
        dpg.add_combo(
            label="Device", tag="medaka_devices",
            default_value="", items=model.get_devices()
        )
        dpg.add_combo(
            label="Guppy Version", tag="medaka_guppy",
            default_value="", items=model.get_guppy_versions()
        )


#################### Callbacks ####################

def _toggle_flye(sender):
    dpg.configure_item("racon_skip", show=dpg.get_value(sender))

def _toggle_medaka_model(sender) -> None:
    state = dpg.get_value(sender)
    dpg.configure_item(
        "medaka_flowcell",
        enabled= not state, no_arrow_button=state
    )
    dpg.configure_item(
        "medaka_devices",
        enabled=not state, no_arrow_button=state
    )
    dpg.configure_item(
        "medaka_guppy",
        enabled=not state, no_arrow_button=state
    )
    dpg.configure_item("medaka_manumodel", show=state)
