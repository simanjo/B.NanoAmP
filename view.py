import os
import webbrowser

import dearpygui.dearpygui as dpg

import model
import controller


def add_file_dialog():
    dpg.add_file_dialog(
        label="Select Base Folder",
        directory_selector=True, show=False,
        callback=_choose_dir, tag="file_dialog",
        width=500, height=400
    )


def add_main_window():
    with dpg.window(
        tag="main_window", autosize=True, no_close=True, no_collapse=True
    ):
        with dpg.tab_bar(tag="tab_bar"):
            with dpg.tab(
                label="Assembly Settings", tag="main_tab", show=False
            ):
                _add_general_settings()
                _add_filtlong_settings()
                _add_assembler_settings()
                _add_medaka_settings()
                dpg.add_button(
                    label="Execute Assembly Pipeline",
                    callback=controller.execute_pipeline
                )


def check_conda():
    if controller.get_conda_version() is None:
        with dpg.window(
            label="Missing Conda", autosize=True,
            no_close=True, no_collapse=True, tag="conda_missing"
        ):
            msg = "We could not find conda in your PATH or "
            msg += "in the standard installation directories.\n"
            msg += "Please supply a valid location for conda, "
            msg += "or install a conda distribution (ie. miniconda) "
            msg += "and try again."
            dpg.add_text(msg)
            dpg.add_spacer(height=20)
            with dpg.group(horizontal=True):
                dpg.add_button(
                    label="Select conda path", callback=_add_conda_path
                )
                dpg.add_button(
                    label="Get miniconda", callback=_miniconda_link
                )
                dpg.add_button(label="Abort", callback=dpg.stop_dearpygui)
    else:
        check_env_setup()


def check_env_setup(force=False):
    envs, prefs = controller.get_conda_setup()
    status, missing = controller.check_pkgs(envs)
    dpg.configure_item("main_tab", show=True)
    if status == "complete" and not force:
        controller.set_conda_envs(envs, prefs)
        dpg.configure_item("medaka_manumodel", items=model.get_models())
        with dpg.tab(
            label="Conda Setup", tag="conda_tab", parent="tab_bar",
        ):
            _display_conda_setup(envs)
        return
    with dpg.window(
        modal=True, label="Checking Conda Setup", autosize=True,
        no_close=True, no_collapse=True, tag="conda_check"
    ):
        _display_conda_setup(envs)
        dpg.add_spacer(height=20)
        msg = "Your conda setup is missing the required packages"
        msg += f"\n{missing}.\n"
        msg += "Do you want to perform a fresh package setup now? "
        msg += "(This might take a while...)"
        dpg.add_text(msg)
        dpg.add_spacer(height=20)
        with dpg.group(horizontal=True):
            dpg.add_button(label="Yes", callback=_handle_conda_init)
            dpg.add_button(label="Abort", callback=dpg.stop_dearpygui)
        with dpg.group(horizontal=True, show=False, tag="progress_ind"):
            dpg.add_text(tag="log_text")
            dpg.add_loading_indicator()


def _miniconda_link():
    webbrowser.open("https://docs.conda.io/en/latest/miniconda.html", new=2)
    dpg.stop_dearpygui()


def _add_conda_path():
    def _choose_conda_dir(sender, app_data):
        try:
            fpath = list(app_data['selections'].values())[0]
        except KeyError:
            return
        if not os.path.isdir(fpath):
            fpath = os.path.abspath(
                os.path.join(fpath, os.pardir)
            )
        assert os.path.isdir(fpath)
        model.PREFIXES['conda'] = fpath
        if controller.get_conda_version() is not None:
            dpg.configure_item("conda_missing", show=False)
            check_env_setup()

    dpg.configure_item("conda_missing", show=True)
    dpg.add_file_dialog(
        label="Select Conda Binary Folder",
        directory_selector=True, callback=_choose_conda_dir,
        width=500, height=400, modal=True
    )


def _handle_conda_init(sender):
    dpg.configure_item("progress_ind", show=True)
    controller.init_conda_envs()
    dpg.configure_item("conda_check", show=False)
    dpg.configure_item("medaka_manumodel", items=model.get_models())
    with dpg.tab(
        label="Conda Setup", tag="conda_tab", parent="tab_bar",
    ):
        _display_conda_setup(controller.get_conda_setup()[0])


def _display_conda_setup(envs):
    dpg.add_text("The following conda setup has been found:")
    with dpg.table():
        dpg.add_table_column(label="environment")
        dpg.add_table_column(label="packages")

        for name, pkgs in envs.items():
            with dpg.table_row():
                with dpg.table_cell():
                    dpg.add_text(name)
                with dpg.table_cell():
                    for pkg in pkgs:
                        dpg.add_text(pkg)

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

    dpg.set_value("bcfolder", fpath)
    dpg.configure_item("bcfolder", show=True)


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
    dpg.add_checkbox(
        label="keep intermediate results",
        tag="keep_intermediate",
        default_value=False
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
            tag="use_Flye",
            default_value=True,
            callback=_toggle_flye
        )
        dpg.add_checkbox(
            label="skip racon polishing",
            tag="racon_skip", default_value=True
        )
    dpg.add_checkbox(
        label="Raven",
        tag="use_Raven",
        default_value=False
    )
    dpg.add_checkbox(
        label="Miniasm",
        tag="use_Miniasm",
        default_value=False
    )
    dpg.add_separator()


def _add_medaka_settings():
    dpg.add_text("Medaka Settings:")
    with dpg.group(tag="medaka_automodel"):
        dpg.add_combo(
            label="FlowCell", tag="medaka_cell",
            default_value="--", items=["--"] + model.get_flow_cells(),
            callback=_change_model_param
        )
        dpg.add_combo(
            label="Device", tag="medaka_device",
            default_value="--", items=["--"] + model.get_devices(),
            callback=_change_model_param
        )
        dpg.add_combo(
            label="Guppy Version", tag="medaka_guppy",
            default_value="--", items=["--"] + model.get_guppy_versions(),
            callback=_change_model_param
        )
        dpg.add_combo(
            label="Basecaller settings", tag="medaka_variant",
            default_value="--", items=["--"] + model.get_guppy_variants(),
            callback=_change_model_param
        )
    with dpg.group(horizontal=True):
        dpg.add_combo(
            label="Model", tag="medaka_manumodel",
            default_value="--", enabled=False, no_arrow_button=True,
            callback=_select_medaka_model
        )
        dpg.add_checkbox(
            label="choose model manually",
            tag="medaka_choose", default_value=False,
            callback=_toggle_medaka_model
        )
    dpg.add_separator()


#################### Callbacks ####################


def _toggle_flye(sender):
    dpg.configure_item("racon_skip", show=dpg.get_value(sender))


def _toggle_medaka_model(sender) -> None:
    state = dpg.get_value(sender)
    dpg.configure_item(
        "medaka_cell",
        enabled=not state, no_arrow_button=state
    )
    dpg.configure_item(
        "medaka_device",
        enabled=not state, no_arrow_button=state
    )
    dpg.configure_item(
        "medaka_guppy",
        enabled=not state, no_arrow_button=state
    )
    dpg.configure_item(
        "medaka_variant",
        enabled=not state, no_arrow_button=state
    )
    dpg.configure_item(
        "medaka_manumodel",
        enabled=state, no_arrow_button=not state
    )


def _select_medaka_model(sender):
    mod = dpg.get_value(sender)
    model_row = model.get_model_df().query(f"full_model == '{mod}'")
    for name in "device", "cell", "guppy", "variant":
        choice = model.get_display_names(name, model_row[name])[0]
        dpg.set_value("medaka_" + name, choice)
        dpg.configure_item("medaka_" + name, items=["--", choice])


def _change_model_param(sender):
    field_missing = False
    kwargs = {}
    for name in ["device", "cell", "guppy", "variant"]:
        dpg_name = "medaka_" + name
        if (val := dpg.get_value(dpg_name)) == "--":
            kwargs[name] = None
            field_missing = True
        else:
            kwargs[name] = model.get_param_name(name, val)
    update = controller.filter_models(**kwargs)
    models = list(update['full_model'])
    if not field_missing or len(models) == 1:
        dpg.set_value("medaka_manumodel", models[0])
        for name in "device", "cell", "guppy", "variant":
            choice = model.get_display_names(name, update[name])[0]
            dpg.set_value("medaka_" + name, choice)
    else:
        dpg.set_value("medaka_manumodel", "--")
    for name in "device", "cell", "guppy", "variant":
        choice = ["--"] + model.get_display_names(name, update[name])
        dpg.configure_item("medaka_" + name, items=choice)
