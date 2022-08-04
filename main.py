#! /usr/bin/env python3

import dearpygui.dearpygui as dpg

import view
from themes import custom_theme

def _init_app():

    view.add_file_dialog()
    view.add_main_window()


def _start_app():
    dpg.bind_theme(custom_theme())
    dpg.create_viewport(
        title='Bacterial Nanopore Assembly Pipeline',
        width=850, height=800
    )
    dpg.setup_dearpygui()

    dpg.show_viewport()
    dpg.set_primary_window("main_window", True)

    dpg.start_dearpygui()


def main():


    dpg.create_context()

    _init_app()
    _start_app()

    dpg.destroy_context()


if __name__ == '__main__':
    main()

