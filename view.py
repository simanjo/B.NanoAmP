import os

import dearpygui.dearpygui as dpg


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

def add_file_dialog():
    dpg.add_file_dialog(
        label="Select Base Folder",
        directory_selector=True, show=False,
        callback=_choose_dir, tag="file_dialog",
        width=500, height=400
    )

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

def add_main_window():
    with dpg.window(tag="main_window", autosize=True, no_close=True, no_collapse=True):
        with dpg.tab_bar():
            with dpg.tab(label="Assembly Settings", tag="main_tab"):
                _add_general_settings()
