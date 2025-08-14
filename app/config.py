import logging
import streamlit as st

logger = logging.getLogger(__name__)


class StreamlitLogHandler(logging.Handler):
    def __init__(self, widget_update_func):
        super().__init__()
        self.widget_update_func = widget_update_func

    def emit(self, record):
        msg = self.format(record)
        self.widget_update_func(msg)


# configure streamlit logger
def add_to_logs(msg):
    st.session_state["logs"] += str(msg) + "\n"


logger.handlers.clear()

streamlit_log_handler = StreamlitLogHandler(add_to_logs)
streamlit_log_handler.setLevel(logging.INFO)
if streamlit_log_handler not in logger.handlers:
    logger.addHandler(streamlit_log_handler)


# Configure logging
FORMAT = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s"
logging.basicConfig(level=logging.INFO, encoding="utf-8", format=FORMAT)
