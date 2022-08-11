import dearpygui.dearpygui as dpg

class ErrorWindow():

    def __init__(self, message, abort=False) -> None:
        self.message = message
        self.abort = abort
        self.window = dpg.add_window(
            modal=True, label="Error", autosize=True, no_close=True,
            no_collapse=True, tag="error_window"
        )
        self._show()

    def _show(self):
        dpg.add_text(self.message, parent=self.window)
        button_text = "Abort" if self.abort else "Ok"
        dpg.add_button(
            label=button_text, callback=self._close, parent=self.window
        )

    def _close(self):
        if self.abort:
            dpg.stop_dearpygui()
        else:
            dpg.delete_item(self.window)