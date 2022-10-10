from logging import Handler

from dearpygui import dearpygui as dpg

class CustomUILogHandler(Handler):

    def __init__(self, parent_id, **kwargs) -> None:
        super().__init__(**kwargs)
        self.parent_id = parent_id

    def emit(self, msg):
        msg=self.format(msg)
        dpg.add_text(msg, parent=self.parent_id)
        dpg.set_y_scroll("log_window", -1.0)

    def flush(self):
        dpg.delete_item(self.parent_id, children_only=True)